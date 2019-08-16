# resumeUpload

- 主文件只有一个main文件，作用为断点上传指定目录下的所有 pdf 文件
- 编译好的 uploader_v_x_win.exe 是可在windows下运行的可执行文件，采用 "-f configPath" 指定配置文件,比如：
`uploader_v_2_win.exe -f ./uploader_win.conf`
- PDFStorage 文件夹中存放可用的 pdf 文件，使用时拷贝进 resumeUploaderTest 目录

## 本地测试
- 当前文件以及子目录下的文件上传成功
- 添加新文件至目录上传成功
- 断点续传成功
- 改变文件 ModTime 等待 prepare 成功
- 同名文件再次添加，日志中会显示上传，但是空间中并不会出现重复。