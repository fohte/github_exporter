package action

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v84/github"
	"github.com/oklog/run"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/promhippie/github_exporter/pkg/config"
	"github.com/promhippie/github_exporter/pkg/store"
	"github.com/promhippie/github_exporter/pkg/version"
	"golang.org/x/oauth2"
)

// Server handles the server sub-command.
func Server(cfg *config.Config, db store.Store, logger *slog.Logger) error {
	logger.Info("Launching GitHub Exporter",
		"version", version.String,
		"revision", version.Revision,
		"date", version.Date,
		"go", version.Go,
	)

	client, err := getClient(cfg, logger)

	if err != nil {
		return err
	}

	var gr run.Group

	{
		server := &http.Server{
			Addr:         cfg.Server.Addr,
			Handler:      handler(cfg, db, logger, client),
			ReadTimeout:  5 * time.Second,
			WriteTimeout: cfg.Server.Timeout,
		}

		gr.Add(func() error {
			logger.Info("Starting metrics server",
				"address", cfg.Server.Addr,
			)

			return web.ListenAndServe(
				server,
				&web.FlagConfig{
					WebListenAddresses: sliceP([]string{cfg.Server.Addr}),
					WebSystemdSocket:   boolP(false),
					WebConfigFile:      stringP(cfg.Server.Web),
				},
				logger,
			)
		}, func(reason error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := server.Shutdown(ctx); err != nil {
				logger.Error("Failed to shutdown metrics gracefully",
					"err", err,
				)

				return
			}

			logger.Info("Metrics shutdown gracefully",
				"reason", reason,
			)
		})
	}

	{
		stop := make(chan os.Signal, 1)

		gr.Add(func() error {
			signal.Notify(stop, os.Interrupt)

			<-stop

			return nil
		}, func(_ error) {
			close(stop)
		})
	}

	return gr.Run()
}

func useEnterprise(cfg *config.Config, _ *slog.Logger) bool {
	return cfg.Target.BaseURL != ""
}

func useApplication(cfg *config.Config, _ *slog.Logger) bool {
	return cfg.Target.PrivateKey != "" && cfg.Target.AppID != 0 && cfg.Target.InstallID != 0
}

func getClient(cfg *config.Config, logger *slog.Logger) (*github.Client, error) {
	if useEnterprise(cfg, logger) {
		return getEnterprise(cfg, logger)
	}

	if useApplication(cfg, logger) {
		privateKey, err := config.Value(cfg.Target.PrivateKey)

		if err != nil {
			logger.Error("Failed to read GitHub key",
				"err", err,
			)

			return nil, err
		}

		transport, err := ghinstallation.New(
			http.DefaultTransport,
			cfg.Target.AppID,
			cfg.Target.InstallID,
			[]byte(privateKey),
		)

		if err != nil {
			logger.Error("Failed to create GitHub transport",
				"err", err,
			)

			return nil, err
		}

		return github.NewClient(
			&http.Client{
				Transport: transport,
			},
		), nil
	}

	accessToken, err := config.Value(cfg.Target.Token)

	if err != nil {
		logger.Error("Failed to read token",
			"err", err,
		)

		return nil, err
	}

	return github.NewClient(
		oauth2.NewClient(
			context.Background(),
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: accessToken,
				},
			),
		),
	), nil
}

func getEnterprise(cfg *config.Config, logger *slog.Logger) (*github.Client, error) {
	if useApplication(cfg, logger) {
		privateKey, err := config.Value(cfg.Target.PrivateKey)

		if err != nil {
			logger.Error("Failed to read GitHub key",
				"err", err,
			)

			return nil, err
		}

		transport, err := ghinstallation.New(
			http.DefaultTransport,
			cfg.Target.AppID,
			cfg.Target.InstallID,
			[]byte(privateKey),
		)

		if err != nil {
			logger.Error("Failed to create GitHub transport",
				"err", err,
			)

			return nil, err
		}

		if !strings.HasSuffix(cfg.Target.BaseURL, "/api/v3") &&
			!strings.HasSuffix(cfg.Target.BaseURL, "/api/v3/") {
			transport.BaseURL = cfg.Target.BaseURL + "/api/v3"
		} else {
			transport.BaseURL = cfg.Target.BaseURL
		}

		client, err := github.NewClient(
			&http.Client{
				Transport: transport,
			},
		).WithEnterpriseURLs(
			cfg.Target.BaseURL,
			cfg.Target.BaseURL,
		)

		if err != nil {
			logger.Error("Failed to parse base URL",
				"err", err,
			)

			return nil, err
		}

		return client, err
	}

	accessToken, err := config.Value(cfg.Target.Token)

	if err != nil {
		logger.Error("Failed to read token",
			"err", err,
		)

		return nil, err
	}

	client, err := github.NewClient(
		oauth2.NewClient(
			context.WithValue(
				context.Background(),
				oauth2.HTTPClient,
				&http.Client{
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{
							InsecureSkipVerify: cfg.Target.Insecure,
						},
					},
				},
			),
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: accessToken,
				},
			),
		),
	).WithEnterpriseURLs(
		cfg.Target.BaseURL,
		cfg.Target.BaseURL,
	)

	if err != nil {
		logger.Error("Failed to parse base URL",
			"err", err,
		)

		return nil, err
	}

	return client, err
}
