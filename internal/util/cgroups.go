// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var cgroupSyncOnce sync.Once

// CgroupVersion represents the cgroup version in use
type CgroupVersion int

var (
	cgroupV1CpuQuotaFile   = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	cgroupV1MemLimitFile   = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cgroupV2CpuQuotaFile   = "/sys/fs/cgroup/cpu.max"
	cgroupV2MemLimitFile   = "/sys/fs/cgroup/memory.max"
	cgroupV2ControllerFile = "/sys/fs/cgroup/cgroup.controllers"
)

const (
	CgroupV1 CgroupVersion = iota
	CgroupV2
)

var detectedCgroupVersion CgroupVersion

// detectCgroupVersion determines if the system is using cgroup v1 or v2
func detectCgroupVersion() CgroupVersion {
	// Check if cgroup v2 is mounted by looking for the unified hierarchy marker
	if _, err := os.Stat(cgroupV2ControllerFile); err == nil {
		return CgroupV2
	}
	// Default to v1
	return CgroupV1
}

func readCgroupLimits() {
	cgroupSyncOnce.Do(func() {
		detectedCgroupVersion = detectCgroupVersion()

		// Read memory limit
		var memoryLimitFile string
		if detectedCgroupVersion == CgroupV2 {
			memoryLimitFile = cgroupV2MemLimitFile
		} else {
			memoryLimitFile = cgroupV1MemLimitFile
		}

		contents, err := os.ReadFile(memoryLimitFile)
		if err == nil {
			contentStr := strings.TrimSpace(string(contents))
			if contentStr == "max" {
				Logger.Debugf("No memory limit detected in %s; assuming unlimited", memoryLimitFile)
				setMaxMemory(-1) // Use -1 to indicate an unknown limit
			} else {
				maxmem, err := strconv.ParseFloat(contentStr, 64)
				if err == nil {
					setMaxMemory(maxmem)
					Logger.Debugf("Max memory limit: %.0f bytes", maxMemory)
				} else {
					Logger.Debugf("Failed to parse memory limit from %s: %v", memoryLimitFile, err)
					setMaxMemory(-1) // Use -1 to indicate an unknown limit
				}
			}
		}

		// Read CPU limit
		var cpuQuotaFile string
		if detectedCgroupVersion == CgroupV2 {
			cpuQuotaFile = cgroupV2CpuQuotaFile
		} else {
			cpuQuotaFile = cgroupV1CpuQuotaFile
		}

		contents, err = os.ReadFile(cpuQuotaFile)
		if err == nil {
			contentStr := strings.TrimSpace(string(contents))
			fields := strings.Fields(contentStr)
			// max is cgroup v2's way of saying "no limit", while -1 is cgroup v1's way, so check for both
			if len(fields) > 0 && fields[0] != "max" && fields[0] != "-1" {
				cpuQuota, err := strconv.ParseFloat(fields[0], 64)
				if err == nil {
					setCPUMillicoreLimit(int(cpuQuota / 100.0))
					Logger.Debugf("CPU millicore limit from %s: %d mCPU", cpuQuotaFile, cpuMillicoreLimit)
				} else {
					Logger.Debugf("Failed to parse CPU quota from %s: %v", cpuQuotaFile, err)
					setCPUMillicoreLimit(-1) // Use -1 to indicate an unknown limit
				}
			} else {
				setCPUMillicoreLimit(runtime.NumCPU() * 1000)
				Logger.Debugf("No CPU limit detected in %s; assuming %d mCPU", cpuQuotaFile, cpuMillicoreLimit)
			}
		}
	})
}
