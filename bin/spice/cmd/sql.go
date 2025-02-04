package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Start an interactive SQL query session against the Spice.ai runtime",
	Example: `
$ spice sql
Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.

show tables;  -- list available tables
sql> show tables
+---------------+--------------------+---------------+------------+
| table_catalog | table_schema       | table_name    | table_type |
+---------------+--------------------+---------------+------------+
| datafusion    | public             | tmp_view_test | VIEW       |
| datafusion    | information_schema | tables        | VIEW       |
| datafusion    | information_schema | views         | VIEW       |
| datafusion    | information_schema | columns       | VIEW       |
| datafusion    | information_schema | df_settings   | VIEW       |
+---------------+--------------------+---------------+------------+
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		execCmd, err := rtcontext.GetRunCmd()
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		execCmd.Args = append(execCmd.Args, "--repl")

		execCmd.Stderr = os.Stderr
		execCmd.Stdout = os.Stdout
		execCmd.Stdin = os.Stdin

		err = util.RunCommand(execCmd)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	sqlCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(sqlCmd)
}
