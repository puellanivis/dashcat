package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/puellanivis/breton/lib/files/httpfiles"
	_ "github.com/puellanivis/breton/lib/files/plugins"
	log "github.com/puellanivis/breton/lib/glog"
	flag "github.com/puellanivis/breton/lib/gnuflag"
	"github.com/puellanivis/breton/lib/io/bufpipe"
	_ "github.com/puellanivis/breton/lib/metrics/http"
	"github.com/puellanivis/breton/lib/net/dash"
	"github.com/puellanivis/breton/lib/util"

	"github.com/pkg/errors"
)

var Flags struct {
	MimeTypes []string `flag:"mime-type,short=t"    desc:"which mime-type(s) to stream (default \"video/mp4\")"`
	Play      bool     `                            desc:"start a subprocess to pipe the output to (currently only mpv)"`
	Quiet     bool     `flag:",short=q"             desc:"suppress unnecessary output from subprocesses"`
	Metrics   bool     `                            desc:"listens on a given port to report metrics"`
	Port      int      `flag:",short=p"             desc:"which port to listen to, if set, implies --metrics (default random available port)"`
	UserAgent string   `flag:",default=dashcat/1.0" desc:"which User-Agent string to use"`
}

func init() {
	flag.FlagStruct("", &Flags)
}

var stderr = os.Stderr

func main() {
	finish, ctx := util.Init("dash-cat", 0, 1)
	defer finish()

	ctx = httpfiles.WithUserAgent(ctx, Flags.UserAgent)

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	defer func() {
		cancel()
		<-done
	}()

	args := flag.Args()
	if len(args) < 1 {
		util.Statusln(flag.Usage)
		return
	}

	if Flags.Quiet {
		stderr = nil
	}

	if Flags.Metrics || Flags.Port != 0 {
		go func() {
			l, err := net.Listen("tcp", fmt.Sprintf(":%d", Flags.Port))
			if err != nil {
				log.Error("failed to establish listener: ", err)
				return
			}

			_, lport, err := net.SplitHostPort(l.Addr().String())
			if err != nil {
				log.Error("failed to get port from listener: ", err)
				return
			}

			msg := fmt.Sprintf("metrics available at: http://localhost:%s/metrics/prometheus", lport)
			util.Statusln(msg)
			log.Infoln(msg)

			srv := &http.Server{}

			go func() {
				<-ctx.Done()
				srv.Shutdown(util.Context())
				l.Close()
			}()

			if err := srv.Serve(l); err != nil {
				if err != http.ErrServerClosed {
					log.Error(err)
				}
			}
		}()
	}

	if !Flags.Play {
		// close done, because there will be no subprocess
		close(done)
	}

	if len(Flags.MimeTypes) < 1 {
		Flags.MimeTypes = append(Flags.MimeTypes, "video/mp4")
	}

	var out io.Writer = os.Stdout

	if Flags.Play {
		mpv, err := exec.LookPath("mpv")
		if err != nil {
			log.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, mpv, "-")

		pipe, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		defer pipe.Close()
		out = pipe

		cmd.Stdout = os.Stdout
		cmd.Stderr = stderr

		if err := cmd.Start(); err != nil {
			log.Error(err)
		}

		go func() {
			defer close(done)
			defer cancel()

			if err := cmd.Wait(); err != nil {
				log.Error(err)
			}

			if !Flags.Quiet {
				log.Info("subprocess quit")
			}
		}()
	}

	for _, arg := range args {
		for err := range maybeMUX(ctx, out, arg) {
			if err != nil {
				log.Errorf("%+v", err)
			}
		}
	}
}

func maybeMUX(ctx context.Context, out io.Writer, arg string) <-chan error {
	errch := make(chan error)

	go func() {
		defer close(errch)

		var wg sync.WaitGroup
		defer wg.Wait()

		mpd, err := dash.New(ctx, arg)
		if err != nil {
			errch <- err
			return
		}

		if len(Flags.MimeTypes) == 1 {
			errch <- stream(ctx, out, mpd, Flags.MimeTypes[0])
			return
		}

		ffmpegArgs := []string{
			"-nostdin",
		}

		for i := range Flags.MimeTypes {
			if mpd.IsDynamic() {
				ffmpegArgs = append(ffmpegArgs,
					"-thread_queue_size", "1024",
				)
			}

			ffmpegArgs = append(ffmpegArgs, "-i", fmt.Sprintf("/dev/fd/%d", 3+i))
		}

		ffmpegArgs = append(ffmpegArgs,
			"-c", "copy",
			"-copyts",
			"-movflags", "frag_keyframe+empty_moov",
		)

		ffmpegArgs = append(ffmpegArgs,
			"-f", "mp4",
			"-",
		)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		if log.V(5) {
			log.Info("ffmpeg", ffmpegArgs)
		}

		cmd := exec.CommandContext(util.Context(), "ffmpeg", ffmpegArgs...)
		cmd.Stdout = out
		cmd.Stderr = stderr

		for _, mimeType := range Flags.MimeTypes {
			rd, wr, err := os.Pipe()
			if err != nil {
				errch <- errors.WithStack(err)
				return
			}

			cmd.ExtraFiles = append(cmd.ExtraFiles, rd)

			// make a loop-only shadow copy for closures.
			mimeType := mimeType

			pipe := bufpipe.New(ctx)
			wg.Add(1)
			go func() {
				defer wg.Done()

				defer func() {
					errch <- errors.WithStack(wr.Close())
				}()

				// simple enough, bufpipe.Pipe will block on Reads until written to.
				if _, err := io.Copy(wr, pipe); err != nil {
					errch <- errors.WithStack(err)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				defer func() {
					errch <- errors.WithStack(pipe.Close())
				}()

				if err := stream(ctx, pipe, mpd, mimeType); err != nil {
					errch <- err
					cancel()
				}
			}()
		}

		if err := cmd.Run(); err != nil {
			state := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if sig := state.Signal(); sig != -1 {
				if sig == syscall.SIGPIPE {
					// ignore “pipe closed”,
					// which means that what we were catting to a process
					// and that process closed.
					return
				}
			}

			errch <- errors.WithStack(err)
		}
	}()

	return errch
}

func stream(ctx context.Context, out io.Writer, mpd *dash.Manifest, mimeType string) error {
	s, err := mpd.Stream(out, mimeType, dash.PickHighestBandwidth())
	if err != nil {
		return err
	}

	if err := s.Init(ctx); err != nil {
		return err
	}

	var totalDuration time.Duration

	// we will later divide this duration by 2 below, to keep it the right
	// value to ensure we don’t update too often.
	minDuration := mpd.MinimumUpdatePeriod() * 2

readLoop:
	for {
		duration, err := s.Read(ctx)
		totalDuration += duration

		if err != nil {
			if err != io.EOF {
				log.Error(err)
			}

			break
		}

		if duration > 0 {
			if log.V(1) {
				util.Statusln("segments had a duration of:", duration)
			}
		}

		if duration < minDuration {
			duration = minDuration
		}

		select {
		case <-time.After(duration / 2):
		case <-ctx.Done():
			break readLoop
		}
	}

	util.Statusf("%s: total duration: %v\n", mimeType, totalDuration)
	return nil
}
