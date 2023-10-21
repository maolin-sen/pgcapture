package main

import (
	"time"

	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/spf13/cobra"
)

var (
	ControllerListenAddr string
)

func init() {
	rootCmd.AddCommand(controller)
	controller.Flags().StringVarP(&ControllerListenAddr, "ListenAddr", "", ":10000", "the tcp address for grpc server to listen")
}

// 启动controller服务，主要功能是接收用户请求，控制快照的下载。
var controller = &cobra.Command{
	Use:   "controller",
	Short: "grpc api for controlling the dump process",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		control := dblog.NewController(dblog.NewMemoryScheduler(time.Millisecond * 100))
		return serveGRPC(&pb.DBLogController_ServiceDesc, ControllerListenAddr, control, func() {})
	},
}
