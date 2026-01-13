package cli

import (
	"log"

	server "github.com/ab76015/razpravljalnica/pkg/server"
	"github.com/spf13/cobra"
)

var (
	nodeID      string
	nodeHost    string
	nodePort    string
	controlAddr string
)

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Start a data node",
	Run: func(cmd *cobra.Command, args []string) {
		if err := server.StartNode(nodeID, nodeHost, nodePort, controlAddr); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	nodeCmd.Flags().StringVar(&nodeID, "id", "", "Node ID")
	nodeCmd.Flags().StringVar(&nodeHost, "host", "localhost", "Host to bind")
	nodeCmd.Flags().StringVar(&nodePort, "port", "50051", "Port to bind")
	nodeCmd.Flags().StringVar(&controlAddr, "control", "localhost:60051", "Control plane address")
	nodeCmd.MarkFlagRequired("id")

	rootCmd.AddCommand(nodeCmd)
}
