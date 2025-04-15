package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	// Global flags
	verbose bool
)

// Main runs the command
func Main(args []string) error {
	// Root command
	rootCmd := &cobra.Command{
		Use:   "candyfs",
		Short: "CandyFS is a distributed file system",
		Long: `CandyFS is a distributed file system
that provides high availability and performance.`,
	}

	// Global flags
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	// Add all subcommands
	rootCmd.AddCommand(
		versionCmd(),
		formatCmd(),
	)

	// Parse and execute
	rootCmd.SetArgs(args[1:])
	return rootCmd.Execute()
}

// versionCmd returns the version command
func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("CandyFS v0.1")
		},
	}
}