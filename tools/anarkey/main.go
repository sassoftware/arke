package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "", "Path to configuration JSON file (skip config screens)")
	flag.Parse()

	// Initialize logger
	if err := InitLogger(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to initialize logger: %v\n", err)
	}

	var model *Model

	// Load config from file if provided
	if *configFile != "" {
		config, streamConfig, err := LoadConfig(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config file: %v\n", err)
			os.Exit(1)
		}
		model = NewModelWithConfig(config, streamConfig)
	} else {
		model = NewModel()
	}

	p := tea.NewProgram(model, tea.WithAltScreen())

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running load tool: %v\n", err)
		os.Exit(1)
	}
}
