package src

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	prepareLimit = 20
)

type Config struct {
	Path         string
	WalkInterval int
	PreparedTime int
	Bucket       string
	AccessKey    string
	SecretKey    string
	Endpoint     string
	Suffix       string
}

type uploadSvr struct {
	filesPrepared chan string
	rootPath      string
	walkInterval  int
	bucket        string
	accessKey     string
	secretKey     string
	preparedTime  int
	endpoint      string
	uploadedFiles map[string]struct{} //记录已经成功上传的文件，用于保证不会重复上传相同的文件
	uploadedCount int                 //启动服务至今上传成功的文件个数
	suffix        string
}

func New(conf Config) *uploadSvr {
	return &uploadSvr{
		filesPrepared: make(chan string, prepareLimit),
		rootPath:      conf.Path,
		walkInterval:  conf.WalkInterval,
		bucket:        conf.Bucket,
		accessKey:     conf.AccessKey,
		secretKey:     conf.SecretKey,
		preparedTime:  conf.PreparedTime,
		endpoint:      conf.Endpoint,
		uploadedFiles: make(map[string]struct{}),
		uploadedCount: 0,
		suffix:        conf.Suffix,
	}
}

// 实时发现可上传的 pdf 文件
func (s *uploadSvr) loopFindPDF() {
	for {
		err := filepath.Walk(s.rootPath, func(path string, info os.FileInfo, e error) error {
			if e != nil {
				return e
			}
			ext := filepath.Ext(path)
			if !info.IsDir() && ext == s.suffix {
				now := time.Now()
				isPrepared := now.After(info.ModTime().Add(time.Duration(s.preparedTime) * time.Second))
				_, uploaded := s.uploadedFiles[path]
				if isPrepared && !uploaded {
					s.uploadedFiles[path] = struct{}{}
					s.filesPrepared <- path
				} else {
					if !isPrepared {
						fmt.Printf("[Info] %s not prepared.\n", path)
					}
				}
			}
			return nil
		})
		fmt.Println("[Info] Searching for new PDF files...")
		if err != nil {
			fmt.Printf("[Error] An error while walk the directory: %v \n", err)
			return
		}
		time.Sleep(time.Duration(s.walkInterval) * time.Second)
	}

}

func (s *uploadSvr) upload() {
	for path := range s.filesPrepared {
		s.resumeUpload(path)
	}
}

func md5Hex(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

type ProgressRecord struct {
	Progresses []storage.BlkputRet `json:"progresses"`
}

func (s *uploadSvr) resumeUpload(localFile string) {
	bucket := s.bucket
	key := filepath.Base(localFile)

	putPolicy := storage.PutPolicy{
		Scope: bucket,
	}
	mac := qbox.NewMac(s.accessKey, s.secretKey)
	upToken := putPolicy.UploadToken(mac)

	cfg := storage.Config{}
	// 空间对应的机房
	cfg.Zone = &storage.ZoneHuadong
	// 是否使用https域名
	cfg.UseHTTPS = false
	// 上传是否使用CDN上传加速
	cfg.UseCdnDomains = false

	// 必须仔细选择一个能标志上传唯一性的 recordKey 用来记录上传进度
	// 我们这里采用 md5(bucket+key+local_path+local_file_last_modified)+".progress" 作为记录上传进度的文件名
	fileInfo, statErr := os.Stat(localFile)
	if statErr != nil {
		fmt.Println("[Error] Local file state error:", statErr)
		return
	}

	fileSize := fileInfo.Size()
	fileLmd := fileInfo.ModTime().UnixNano()
	recordKey := md5Hex(fmt.Sprintf("%s:%s:%s:%s", bucket, key, localFile, fileLmd)) + ".progress"

	// 指定的进度文件保存目录，实际情况下，请确保该目录存在，而且只用于记录进度文件
	recordDir := filepath.Join(s.rootPath, "progress")
	mErr := os.MkdirAll(recordDir, 0755)
	if mErr != nil {
		fmt.Println("[Error] Mkdir for record dir error:", mErr)
		return
	}

	recordPath := filepath.Join(recordDir, recordKey)

	progressRecord := ProgressRecord{}
	// 尝试从旧的进度文件中读取进度
	recordFp, openErr := os.Open(recordPath)
	if openErr == nil {
		progressBytes, readErr := ioutil.ReadAll(recordFp)
		if readErr == nil {
			mErr := json.Unmarshal(progressBytes, &progressRecord)
			if mErr == nil {
				// 检查context 是否过期，避免701错误
				for _, item := range progressRecord.Progresses {
					if storage.IsContextExpired(item) {
						fmt.Println("[Info] Context exceed the time limit, expired at", item.ExpiredAt)
						progressRecord.Progresses = make([]storage.BlkputRet, storage.BlockCount(fileSize))
						break
					}
				}
			}
		}
		recordFp.Close()
	}

	if len(progressRecord.Progresses) == 0 {
		progressRecord.Progresses = make([]storage.BlkputRet, storage.BlockCount(fileSize))
	} else {
		fmt.Println("[Info] Restart upload from breakpoint...")
	}

	resumeUploader := storage.NewResumeUploader(&cfg)
	ret := storage.PutRet{}
	progressLock := sync.RWMutex{}

	putExtra := storage.RputExtra{
		Progresses: progressRecord.Progresses,
		Notify: func(blkIdx int, blkSize int, ret *storage.BlkputRet) {
			progressLock.Lock()
			progressLock.Unlock()

			//如果上传成功，则将进度序列化，然后写入文件
			progressRecord.Progresses[blkIdx] = *ret
			progressBytes, _ := json.Marshal(progressRecord)
			fmt.Println("[Progress Saving] Write progress file", blkIdx, recordPath)
			wErr := ioutil.WriteFile(recordPath, progressBytes, 0644)
			if wErr != nil {
				fmt.Println("[Error] Write progress file error,", wErr)
			}
		},
		UpHost: s.endpoint,
	}
	err := resumeUploader.PutFile(context.Background(), &ret, upToken, key, localFile, &putExtra)
	if err != nil {
		fmt.Println("[Failed] ", localFile, "upload failed.")
		return
	}
	delete(s.uploadedFiles, localFile)
	s.uploadedCount++
	fmt.Println("[Success] ", localFile, "upload succeed.", s.uploadedCount, "files have uploaded.")

	//删除源文件
	os.Remove(localFile)
	//删除进度文件
	os.Remove(recordPath)
}

func main() {
	fmt.Println("[Info] Mission start.")
	v := viper.New()
	v.SetConfigName("config")
	pwd, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		fmt.Printf("[Error] config load error: %v\n", err)
		return
	}

	v.AddConfigPath(pwd)
	v.SetConfigType("yaml")

	if err = v.ReadInConfig(); err != nil {
		fmt.Printf("[Error] config load error: %v\n", err)
		return
	}

	var conf Config
	err = v.Unmarshal(&conf)
	if err != nil {
		fmt.Printf("[Error] config load error: %v \n", err)
		return
	}
	fmt.Println("[Info] Load config from path", filepath.Join(pwd, "config.yaml"))
	svr := New(conf)
	fmt.Println("[Info] Upload service initialized successfully.")

	go svr.loopFindPDF()
	svr.upload()
}
