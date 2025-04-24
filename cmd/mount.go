package cmd

import (
	"candyfs/pkg/fs"
	"candyfs/pkg/meta"
	"candyfs/utils/log"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
)

func mountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount META_URL MOUNTPOINT",
		Short: "挂载文件系统",
		Long:  `将CandyFS文件系统挂载到指定的挂载点。

使用示例:
  # 前台挂载
  $ candyfs mount redis://localhost:6379 /mnt/candyfs

  # 后台挂载
  $ candyfs mount redis://localhost:6379 /mnt/candyfs -d

  # 挂载带密码保护的Redis
  $ candyfs mount redis://:mypassword@localhost:6379 /mnt/candyfs

  # 环境变量方式提供密码（更安全）
  $ META_PASSWORD=mypassword candyfs mount redis://localhost:6379 /mnt/candyfs

  # 以只读模式挂载
  $ candyfs mount redis://localhost:6379 /mnt/candyfs --read-only`,
		Args: cobra.ExactArgs(2),
		Run:  mount,
	}

	// 添加特定的选项
	cmd.Flags().BoolP("background", "d", false, "在后台运行")
	cmd.Flags().Bool("read-only", false, "只读模式挂载")
	cmd.Flags().String("cache-dir", "/var/cache/candyfs", "缓存目录")
	cmd.Flags().StringP("options", "o", "", "额外的挂载选项")
	cmd.Flags().Uint64("buffer-size", 300, "读写缓冲区大小 (MB)")
	cmd.Flags().Bool("allow-other", false, "允许其他用户访问挂载点")

	return cmd
}

func mount(cmd *cobra.Command, args []string) {
	// 解析参数
	metaUrl := args[0]
	mountPoint := args[1]
	background, _ := cmd.Flags().GetBool("background")
	readOnly, _ := cmd.Flags().GetBool("read-only")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	bufferSize, _ := cmd.Flags().GetUint64("buffer-size")
	allowOther, _ := cmd.Flags().GetBool("allow-other")

	// 确保挂载点存在
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		log.Fatalf("创建挂载点失败: %s", err)
	}

	// 获取挂载点的绝对路径
	mountPoint, err := filepath.Abs(mountPoint)
	if err != nil {
		log.Fatalf("获取挂载点绝对路径失败: %s", err)
	}

	log.Infof("正在挂载 %s 到 %s", metaUrl, mountPoint)

	// 连接元数据服务
	conf := meta.DefaultConf()
	conf.ReadOnly = readOnly
	conf.MountPoint = mountPoint

	metaClient := meta.NewClient(metaUrl, conf)

	// 创建并挂载FUSE文件系统
	candyFS := fs.NewCandyFS(metaClient)
	if err := candyFS.Mount(mountPoint, allowOther); err != nil {
		log.Fatalf("FUSE挂载失败: %s", err)
	}

	// 显示挂载信息
	fmt.Printf("元数据服务: %s\n", metaClient.Name())
	fmt.Printf("挂载点: %s\n", mountPoint)
	fmt.Printf("缓存目录: %s\n", cacheDir)
	fmt.Printf("缓冲区大小: %d MB\n", bufferSize)
	if readOnly {
		fmt.Println("挂载模式: 只读")
	} else {
		fmt.Println("挂载模式: 读写")
	}

	// 在前台模式下, 等待文件系统关闭
	if !background {
		fmt.Println("\n挂载成功，按 Ctrl+C 卸载")
		
		// 捕获信号
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		
		// 等待信号
		<-c
		fmt.Println("\n正在卸载...")
		
		// 卸载文件系统
		if err := candyFS.Unmount(); err != nil {
			log.Errorf("卸载失败: %s", err)
		} else {
			fmt.Println("文件系统已卸载")
		}
	} else if runtime.GOOS != "windows" {
		// 后台运行模式（守护进程）
		fmt.Println("后台模式已启动，使用以下命令卸载:")
		fmt.Printf("  fusermount -u %s\n", mountPoint)
		
		// 如果在后台运行，启动FUSE服务但不等待关闭
		candyFS.Serve()
	}
}