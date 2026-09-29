package provider

import "github.com/sassoftware/arke/internal/util"

func SleepRandomReconnect() {
	util.SleepRandom(100, ReconnectDelay)
}
