package cmd

import (
	"candyfs/pkg/meta"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// formatCmd returns the format command
func formatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "format",
		Short: "Format storage for CandyFS",
		Long:  "Format storage locations for use with CandyFS. This is required before first use.",
		Run:   format,
	}

	// Command-specific flags
	cmd.Flags().StringP("path", "p", "", "Path to storage location (required)")
	cmd.MarkFlagRequired("path")
	cmd.Flags().BoolP("force", "f", false, "Force format even if storage is not empty")

	return cmd
}

func format(cmd *cobra.Command, args []string) {
	// 获取必要的参数
	path, _ := cmd.Flags().GetString("path")
	force, _ := cmd.Flags().GetBool("force")
	
	if path == "" {
		fmt.Println("错误: 必须指定存储路径")
		os.Exit(1)
	}
	
	fmt.Printf("正在格式化存储路径: %s\n", path)
	
	// 创建元数据客户端
	conf := meta.DefaultConf()
	conf.ReadOnly = false // 确保不是只读模式
	
	// 确认操作
	if !force {
		fmt.Println("警告: 格式化操作将清除所有现有数据，请确认路径正确")
		fmt.Println("提示: 如果确认要格式化，请使用 --force 参数")
		os.Exit(1)
	}
	
	// 创建元数据客户端并连接
	m := meta.NewClient(path, conf)
	
	// 执行格式化
	if err := m.Format(); err != nil {
		fmt.Printf("格式化失败: %s\n", err)
		os.Exit(1)
	}
	
	fmt.Println("格式化成功完成")
}
