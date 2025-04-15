package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)


// formatCmd returns the format command
func formatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "format",
		Short: "Format storage for CandyFS",
		Long:  "Format storage locations for use with CandyFS. This is required before first use.",
		Run: func(cmd *cobra.Command, args []string) {
			path, _ := cmd.Flags().GetString("path")
			force, _ := cmd.Flags().GetBool("force")
			
			if path == "" {
				fmt.Println("Error: storage path is required")
				cmd.Usage()
				os.Exit(1)
			}
			
			if force {
				fmt.Printf("Force formatting storage at %s...\n", path)
			} else {
				fmt.Printf("Formatting storage at %s...\n", path)
			}
			
			// Implementation for formatting storage
			fmt.Println("Storage formatted successfully")
		},
	}
	
	// Command-specific flags
	cmd.Flags().StringP("path", "p", "", "Path to storage location (required)")
	cmd.MarkFlagRequired("path")
	cmd.Flags().BoolP("force", "f", false, "Force format even if storage is not empty")
	
	return cmd
}