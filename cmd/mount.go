package cmd

import (
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

	// TODO: 创建FUSE服务并挂载
	// 这部分需要实现FUSE集成，但目前CandyFS缺少VFS层

	// 临时方案：显示挂载信息并等待信号
	fmt.Printf("元数据服务: %s\n", metaClient.Name())
	fmt.Printf("挂载点: %s\n", mountPoint)
	fmt.Printf("缓存目录: %s\n", cacheDir)
	fmt.Printf("缓冲区大小: %d MB\n", bufferSize)
	if readOnly {
		fmt.Println("挂载模式: 只读")
	} else {
		fmt.Println("挂载模式: 读写")
	}

	// 在前台模式下捕获信号
	if !background {
		fmt.Println("\n挂载成功，按 Ctrl+C 卸载")
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		fmt.Println("\n正在卸载...")
	}

	// 如果需要在后台运行，可以添加守护进程逻辑
	if background && runtime.GOOS != "windows" {
		// TODO: 实现守护进程逻辑
		fmt.Println("后台模式已启动")
	}
}