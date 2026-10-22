// Command token issues a viewer token for local testing: token -user alice -mod bobs_channel
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aaryanpallath/chorus/internal/edge"
)

func main() {
	user := flag.String("user", "", "user name")
	mod := flag.String("mod", "", "comma-separated channels the user moderates")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	flag.Parse()
	secret := os.Getenv("CHORUS_SECRET")
	if *user == "" || secret == "" {
		fmt.Fprintln(os.Stderr, "usage: CHORUS_SECRET=... token -user NAME [-mod ch1,ch2]")
		os.Exit(2)
	}
	var mods []string
	if *mod != "" {
		mods = strings.Split(*mod, ",")
	}
	fmt.Println(edge.Sign([]byte(secret), edge.Claims{User: strings.ToLower(*user), ModOf: mods,
		Expires: time.Now().Add(*ttl).Unix()}))
}
