// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// Helper function to reset the sync.Once for testing
func resetCgroupSync() {
	cgroupSyncOnce = sync.Once{}
}

// Helper function to setup temp cgroup files
func setupTempCgroupFiles(t *testing.T, version CgroupVersion, memLimit, cpuQuota string) string {
	t.Helper()
	tempDir := t.TempDir()

	if version == CgroupV2 {
		// Create v2 structure
		err := os.WriteFile(filepath.Join(tempDir, "cgroup.controllers"), []byte("cpu memory io"), 0600)
		if err != nil {
			t.Fatalf("Failed to create controller file: %v", err)
		}

		if memLimit != "" {
			err = os.WriteFile(filepath.Join(tempDir, "memory.max"), []byte(memLimit), 0600)
			if err != nil {
				t.Fatalf("Failed to create memory limit file: %v", err)
			}
		}

		if cpuQuota != "" {
			err = os.WriteFile(filepath.Join(tempDir, "cpu.max"), []byte(cpuQuota), 0600)
			if err != nil {
				t.Fatalf("Failed to create cpu quota file: %v", err)
			}
		}
	} else {
		// Create v1 structure
		cpuDir := filepath.Join(tempDir, "cpu")
		memDir := filepath.Join(tempDir, "memory")

		if err := os.MkdirAll(cpuDir, 0755); err != nil {
			t.Fatalf("Failed to create cpu directory: %v", err)
		}
		if err := os.MkdirAll(memDir, 0755); err != nil {
			t.Fatalf("Failed to create memory directory: %v", err)
		}

		if memLimit != "" {
			err := os.WriteFile(filepath.Join(memDir, "memory.limit_in_bytes"), []byte(memLimit), 0600)
			if err != nil {
				t.Fatalf("Failed to create memory limit file: %v", err)
			}
		}

		if cpuQuota != "" {
			err := os.WriteFile(filepath.Join(cpuDir, "cpu.cfs_quota_us"), []byte(cpuQuota), 0600)
			if err != nil {
				t.Fatalf("Failed to create cpu quota file: %v", err)
			}
		}
	}

	return tempDir
}

func TestDetectCgroupVersion(t *testing.T) {
	tests := []struct {
		name             string
		createController bool
		expectedVersion  CgroupVersion
	}{
		{
			name:             "detect cgroup v2 when controller file exists",
			createController: true,
			expectedVersion:  CgroupV2,
		},
		{
			name:             "detect cgroup v1 when controller file missing",
			createController: false,
			expectedVersion:  CgroupV1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory
			tempDir := t.TempDir()

			// Save original value
			originalControllerFile := cgroupV2ControllerFile
			defer func() {
				cgroupV2ControllerFile = originalControllerFile
			}()

			// Set to temp path
			cgroupV2ControllerFile = filepath.Join(tempDir, "cgroup.controllers")

			if tt.createController {
				err := os.WriteFile(cgroupV2ControllerFile, []byte("cpu memory"), 0600)
				if err != nil {
					t.Fatalf("Failed to create controller file: %v", err)
				}
			}

			result := detectCgroupVersion()
			if result != tt.expectedVersion {
				t.Errorf("detectCgroupVersion() = %v, want %v", result, tt.expectedVersion)
			}
		})
	}
}

func TestReadCgroupLimits_CgroupV2(t *testing.T) {
	tests := []struct {
		name                  string
		memLimit              string
		cpuQuota              string
		expectedMemory        float64
		expectedCPUMillicores int
	}{
		{
			name:                  "normal limits v2",
			memLimit:              "1073741824", // 1GB
			cpuQuota:              "200000 100000",
			expectedMemory:        1073741824,
			expectedCPUMillicores: 2000, // 200000/100
		},
		{
			name:                  "max memory unlimited v2",
			memLimit:              "max",
			cpuQuota:              "100000 100000",
			expectedMemory:        0, // remains unchanged
			expectedCPUMillicores: 1000,
		},
		{
			name:                  "max cpu unlimited v2",
			memLimit:              "2147483648", // 2GB
			cpuQuota:              "max 100000",
			expectedMemory:        2147483648,
			expectedCPUMillicores: runtime.NumCPU() * 1000, // defaults to NumCPU
		},
		{
			name:                  "both max v2",
			memLimit:              "max",
			cpuQuota:              "max",
			expectedMemory:        0,
			expectedCPUMillicores: runtime.NumCPU() * 1000,
		},
		{
			name:                  "fractional cpu v2",
			memLimit:              "536870912", // 512MB
			cpuQuota:              "50000 100000",
			expectedMemory:        536870912,
			expectedCPUMillicores: 500, // 50000/100
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset sync.Once to allow readCgroupLimits to run again
			resetCgroupSync()

			// Setup temp files
			tempDir := setupTempCgroupFiles(t, CgroupV2, tt.memLimit, tt.cpuQuota)

			// Save original values
			origV2Controller := cgroupV2ControllerFile
			origV2MemLimit := cgroupV2MemLimitFile
			origV2CpuQuota := cgroupV2CpuQuotaFile
			origMaxMemory := maxMemory
			origCPULimit := cpuMillicoreLimit

			defer func() {
				cgroupV2ControllerFile = origV2Controller
				cgroupV2MemLimitFile = origV2MemLimit
				cgroupV2CpuQuotaFile = origV2CpuQuota
				maxMemory = origMaxMemory
				cpuMillicoreLimit = origCPULimit
			}()

			// Set temp paths
			cgroupV2ControllerFile = filepath.Join(tempDir, "cgroup.controllers")
			cgroupV2MemLimitFile = filepath.Join(tempDir, "memory.max")
			cgroupV2CpuQuotaFile = filepath.Join(tempDir, "cpu.max")

			// Reset global variables
			maxMemory = 0
			cpuMillicoreLimit = 0

			// Run the function
			readCgroupLimits()

			// Verify results
			if tt.expectedMemory > 0 && maxMemory != tt.expectedMemory {
				t.Errorf("maxMemory = %v, want %v", maxMemory, tt.expectedMemory)
			}

			if cpuMillicoreLimit != tt.expectedCPUMillicores {
				t.Errorf("cpuMillicoreLimit = %v, want %v", cpuMillicoreLimit, tt.expectedCPUMillicores)
			}
		})
	}
}

func TestReadCgroupLimits_CgroupV1(t *testing.T) {
	tests := []struct {
		name                  string
		memLimit              string
		cpuQuota              string
		expectedMemory        float64
		expectedCPUMillicores int
	}{
		{
			name:                  "normal limits v1",
			memLimit:              "1073741824", // 1GB
			cpuQuota:              "200000",
			expectedMemory:        1073741824,
			expectedCPUMillicores: 2000, // 200000/100
		},
		{
			name:                  "negative cpu quota (unlimited) v1",
			memLimit:              "2147483648", // 2GB
			cpuQuota:              "-1",
			expectedMemory:        2147483648,
			expectedCPUMillicores: runtime.NumCPU() * 1000, // defaults to NumCPU * 1000
		},
		{
			name:                  "high memory limit v1",
			memLimit:              "9223372036854771712", // Very high limit (basically unlimited)
			cpuQuota:              "100000",
			expectedMemory:        9223372036854771712,
			expectedCPUMillicores: 1000,
		},
		{
			name:                  "small cpu quota v1",
			memLimit:              "536870912", // 512MB
			cpuQuota:              "10000",
			expectedMemory:        536870912,
			expectedCPUMillicores: 100, // 10000/100
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset sync.Once
			resetCgroupSync()

			// Setup temp files for v1
			tempDir := setupTempCgroupFiles(t, CgroupV1, tt.memLimit, tt.cpuQuota)

			// Save original values
			origV2Controller := cgroupV2ControllerFile
			origV1MemLimit := cgroupV1MemLimitFile
			origV1CpuQuota := cgroupV1CpuQuotaFile
			origMaxMemory := maxMemory
			origCPULimit := cpuMillicoreLimit

			defer func() {
				cgroupV2ControllerFile = origV2Controller
				cgroupV1MemLimitFile = origV1MemLimit
				cgroupV1CpuQuotaFile = origV1CpuQuota
				maxMemory = origMaxMemory
				cpuMillicoreLimit = origCPULimit
			}()

			// Set temp paths - make controller file not exist to force v1
			cgroupV2ControllerFile = filepath.Join(tempDir, "nonexistent", "cgroup.controllers")
			cgroupV1MemLimitFile = filepath.Join(tempDir, "memory", "memory.limit_in_bytes")
			cgroupV1CpuQuotaFile = filepath.Join(tempDir, "cpu", "cpu.cfs_quota_us")

			// Reset global variables
			maxMemory = 0
			cpuMillicoreLimit = 0

			// Run the function
			readCgroupLimits()

			// Verify results
			if tt.expectedMemory > 0 && maxMemory != tt.expectedMemory {
				t.Errorf("maxMemory = %v, want %v", maxMemory, tt.expectedMemory)
			}

			if tt.expectedCPUMillicores >= 0 && cpuMillicoreLimit != tt.expectedCPUMillicores {
				t.Errorf("cpuMillicoreLimit = %v, want %v", cpuMillicoreLimit, tt.expectedCPUMillicores)
			}
		})
	}
}

func TestReadCgroupLimits_InvalidData(t *testing.T) {
	tests := []struct {
		name     string
		memLimit string
		cpuQuota string
	}{
		{
			name:     "invalid memory value",
			memLimit: "not-a-number",
			cpuQuota: "100000",
		},
		{
			name:     "invalid cpu quota",
			memLimit: "1073741824",
			cpuQuota: "not-a-number",
		},
		{
			name:     "empty files",
			memLimit: "",
			cpuQuota: "",
		},
		{
			name:     "whitespace only",
			memLimit: "   ",
			cpuQuota: "   ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset sync.Once
			resetCgroupSync()

			// Setup temp files
			tempDir := setupTempCgroupFiles(t, CgroupV2, tt.memLimit, tt.cpuQuota)

			// Save original values
			origV2Controller := cgroupV2ControllerFile
			origV2MemLimit := cgroupV2MemLimitFile
			origV2CpuQuota := cgroupV2CpuQuotaFile
			origMaxMemory := maxMemory
			origCPULimit := cpuMillicoreLimit

			defer func() {
				cgroupV2ControllerFile = origV2Controller
				cgroupV2MemLimitFile = origV2MemLimit
				cgroupV2CpuQuotaFile = origV2CpuQuota
				maxMemory = origMaxMemory
				cpuMillicoreLimit = origCPULimit
			}()

			// Set temp paths
			cgroupV2ControllerFile = filepath.Join(tempDir, "cgroup.controllers")
			cgroupV2MemLimitFile = filepath.Join(tempDir, "memory.max")
			cgroupV2CpuQuotaFile = filepath.Join(tempDir, "cpu.max")

			// Reset global variables
			maxMemory = 0
			cpuMillicoreLimit = 0

			// Run the function - should not panic
			readCgroupLimits()

			// Function should handle errors gracefully
			// maxMemory and cpuMillicoreLimit should remain at default values
		})
	}
}

func TestReadCgroupLimits_MissingFiles(t *testing.T) {
	t.Run("missing memory and cpu files v2", func(t *testing.T) {
		// Reset sync.Once
		resetCgroupSync()

		tempDir := t.TempDir()

		// Only create controller file, not memory or cpu files
		err := os.WriteFile(filepath.Join(tempDir, "cgroup.controllers"), []byte("cpu memory"), 0600)
		if err != nil {
			t.Fatalf("Failed to create controller file: %v", err)
		}

		// Save original values
		origV2Controller := cgroupV2ControllerFile
		origV2MemLimit := cgroupV2MemLimitFile
		origV2CpuQuota := cgroupV2CpuQuotaFile
		origMaxMemory := maxMemory
		origCPULimit := cpuMillicoreLimit

		defer func() {
			cgroupV2ControllerFile = origV2Controller
			cgroupV2MemLimitFile = origV2MemLimit
			cgroupV2CpuQuotaFile = origV2CpuQuota
			maxMemory = origMaxMemory
			cpuMillicoreLimit = origCPULimit
		}()

		// Set temp paths
		cgroupV2ControllerFile = filepath.Join(tempDir, "cgroup.controllers")
		cgroupV2MemLimitFile = filepath.Join(tempDir, "memory.max")
		cgroupV2CpuQuotaFile = filepath.Join(tempDir, "cpu.max")

		// Reset global variables
		maxMemory = 0
		cpuMillicoreLimit = 0

		// Run the function - should not panic
		readCgroupLimits()

		// Should handle missing files gracefully
		// Values should remain at defaults
	})
}

func TestCgroupVersion_Constants(t *testing.T) {
	if CgroupV1 != 0 {
		t.Errorf("CgroupV1 = %v, want 0", CgroupV1)
	}
	if CgroupV2 != 1 {
		t.Errorf("CgroupV2 = %v, want 1", CgroupV2)
	}
}

func TestCgroupFilePaths(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		expected string
	}{
		{
			name:     "cgroupV1CpuQuotaFile",
			variable: cgroupV1CpuQuotaFile,
			expected: "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
		},
		{
			name:     "cgroupV1MemLimitFile",
			variable: cgroupV1MemLimitFile,
			expected: "/sys/fs/cgroup/memory/memory.limit_in_bytes",
		},
		{
			name:     "cgroupV2CpuQuotaFile",
			variable: cgroupV2CpuQuotaFile,
			expected: "/sys/fs/cgroup/cpu.max",
		},
		{
			name:     "cgroupV2MemLimitFile",
			variable: cgroupV2MemLimitFile,
			expected: "/sys/fs/cgroup/memory.max",
		},
		{
			name:     "cgroupV2ControllerFile",
			variable: cgroupV2ControllerFile,
			expected: "/sys/fs/cgroup/cgroup.controllers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.variable != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.variable, tt.expected)
			}
		})
	}
}

func TestReadCgroupLimits_SyncOnce(t *testing.T) {
	t.Run("readCgroupLimits only executes once", func(t *testing.T) {
		// Reset sync.Once
		resetCgroupSync()

		// Setup temp files
		tempDir := setupTempCgroupFiles(t, CgroupV2, "1073741824", "100000 100000")

		// Save original values
		origV2Controller := cgroupV2ControllerFile
		origV2MemLimit := cgroupV2MemLimitFile
		origV2CpuQuota := cgroupV2CpuQuotaFile
		origMaxMemory := maxMemory
		origCPULimit := cpuMillicoreLimit

		defer func() {
			cgroupV2ControllerFile = origV2Controller
			cgroupV2MemLimitFile = origV2MemLimit
			cgroupV2CpuQuotaFile = origV2CpuQuota
			maxMemory = origMaxMemory
			cpuMillicoreLimit = origCPULimit
			resetCgroupSync() // Reset for other tests
		}()

		// Set temp paths
		cgroupV2ControllerFile = filepath.Join(tempDir, "cgroup.controllers")
		cgroupV2MemLimitFile = filepath.Join(tempDir, "memory.max")
		cgroupV2CpuQuotaFile = filepath.Join(tempDir, "cpu.max")

		// Reset global variables
		maxMemory = 0
		cpuMillicoreLimit = 0

		// First call
		readCgroupLimits()
		firstMemory := maxMemory
		firstCPU := cpuMillicoreLimit

		// Change the files
		_ = os.WriteFile(cgroupV2MemLimitFile, []byte("2147483648"), 0600)
		_ = os.WriteFile(cgroupV2CpuQuotaFile, []byte("200000 100000"), 0600)

		// Second call - should not re-read files due to sync.Once
		readCgroupLimits()
		secondMemory := maxMemory
		secondCPU := cpuMillicoreLimit

		// Values should be the same
		if firstMemory != secondMemory {
			t.Errorf("memory changed between calls: first=%v, second=%v", firstMemory, secondMemory)
		}
		if firstCPU != secondCPU {
			t.Errorf("cpu changed between calls: first=%v, second=%v", firstCPU, secondCPU)
		}
	})
}
