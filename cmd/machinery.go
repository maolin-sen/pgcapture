package main

import (
	"encoding/json"
	"github.com/replicase/pgcapture/pkg/dblog"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(machinery)
}

var machinery = &cobra.Command{
	Use:   "machinery",
	Short: "grpc api for machinery scheduler task to worker",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		resolverConfig := map[string]dblog.StaticAgentPulsarURIConfig{}
		if err = json.Unmarshal([]byte(ResolverConfig), &resolverConfig); err != nil {
			return err
		}
		controlConn, err := grpc.Dial(ControllerAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		gateway := &dblog.Gateway{
			SourceResolver: dblog.NewStaticAgentPulsarResolver(resolverConfig),
			DumpInfoPuller: &dblog.GRPCDumpInfoPuller{Client: pb.NewDBLogControllerClient(controlConn)},
		}
		return serveGRPC(&pb.DBLogGateway_ServiceDesc, "", gateway, func() {})
	},
}
