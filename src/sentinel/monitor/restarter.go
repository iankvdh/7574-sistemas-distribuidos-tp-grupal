package monitor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"time"
)

type DockerRestarter struct {
	timeout     time.Duration
	stopTimeout int
}

func NewDockerRestarter(timeout time.Duration, stopTimeoutSeconds int) *DockerRestarter {
	return &DockerRestarter{timeout: timeout, stopTimeout: stopTimeoutSeconds}
}

func (r *DockerRestarter) Restart(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "restart",
		fmt.Sprintf("--time=%d", r.stopTimeout), name)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker restart %s: %w (stderr=%s)", name, err, stderr.String())
	}
	return nil
}
