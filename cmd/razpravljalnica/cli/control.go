package cli

import (
	"log"

	control "github.com/ab76015/razpravljalnica/pkg/control"
	"github.com/spf13/cobra"
)

var controlListenAddr string

var controlCmd = &cobra.Command{
	Use:   "control",
	Short: "Start control plane",
	Run: func(cmd *cobra.Command, args []string) {
		if err := control.StartControl(controlListenAddr); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	controlCmd.Flags().StringVar(&controlListenAddr, "addr", ":60051", "Control plane listen address")
	rootCmd.AddCommand(controlCmd)
}
