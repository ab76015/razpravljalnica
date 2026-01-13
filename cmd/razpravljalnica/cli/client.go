package cli

import (
	"log"

	client "github.com/ab76015/razpravljalnica/pkg/client"
	"github.com/spf13/cobra"
)

var clientAddr string

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Run test client",
	Run: func(cmd *cobra.Command, args []string) {
		if err := client.Run(clientAddr); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	clientCmd.Flags().StringVar(&clientAddr, "addr", "localhost:50051", "Data node address")
	rootCmd.AddCommand(clientCmd)
}
