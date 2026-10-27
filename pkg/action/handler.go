package action

import (
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/go-github/v84/github"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/promhippie/github_exporter/pkg/config"
	"github.com/promhippie/github_exporter/pkg/exporter"
	"github.com/promhippie/github_exporter/pkg/middleware"
	"github.com/promhippie/github_exporter/pkg/store"
)

func handler(cfg *config.Config, db store.Store, logger *slog.Logger, client *github.Client, health *exporter.WorkflowRunHealth) *chi.Mux {
	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer(logger))
	mux.Use(middleware.RealIP)
	mux.Use(middleware.Timeout)
	mux.Use(middleware.Cache)

	if cfg.Server.Pprof {
		mux.Mount("/debug", middleware.Profiler())
	}

	if cfg.Collector.Admin {
		logger.Debug("Admin collector registered")

		registry.MustRegister(exporter.NewAdminCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	if cfg.Collector.Orgs {
		logger.Debug("Org collector registered")

		registry.MustRegister(exporter.NewOrgCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	if cfg.Collector.Repos {
		logger.Debug("Repo collector registered")

		registry.MustRegister(exporter.NewRepoCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	if cfg.Collector.Billing {
		logger.Debug("Billing collector registered")

		registry.MustRegister(exporter.NewBillingCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	if cfg.Collector.Runners {
		logger.Debug("Runner collector registered")

		registry.MustRegister(exporter.NewRunnerCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	if cfg.Collector.WorkflowRuns {
		logger.Debug("WorkflowRun collector registered")

		registry.MustRegister(exporter.NewWorkflowRunCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
			health,
		))
	}

	if cfg.Collector.WorkflowJobs {
		logger.Debug("WorkflowJob collector registered")

		registry.MustRegister(exporter.NewWorkflowJobCollector(
			logger,
			client,
			db,
			requestFailures,
			requestDuration,
			cfg.Target,
		))
	}

	reg := promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			ErrorLog: promLogger{logger},
		},
	)

	mux.NotFound(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, cfg.Server.Path, http.StatusMovedPermanently)
	})

	mux.Route("/", func(root chi.Router) {
		root.Handle(cfg.Server.Path, reg)

		if cfg.Collector.WorkflowRuns || cfg.Collector.WorkflowJobs {
			root.HandleFunc(cfg.Webhook.Path, func(w http.ResponseWriter, r *http.Request) {
				secret, err := config.Value(cfg.Webhook.Secret)

				if err != nil {
					logger.Error("Failed to read webhook secret",
						"error", err,
					)

					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusInternalServerError)

					_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
					return
				}

				payload, err := github.ValidatePayload(
					r,
					[]byte(secret),
				)

				if err != nil {
					logger.Error("Failed to parse github webhook",
						"error", err,
					)

					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusInternalServerError)

					_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
					return
				}

				event, err := github.ParseWebHook(
					github.WebHookType(r),
					payload,
				)

				if err != nil {
					logger.Error("Failed to parse github event",
						"error", err,
					)

					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusInternalServerError)

					_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
					return
				}

				switch event := event.(type) {
				case *github.WorkflowRunEvent:
					wfRun := event.GetWorkflowRun()
					logger.Debug("Received webhook request",
						"type", "workflow_run",
						"owner", event.GetRepo().GetOwner().GetLogin(),
						"repo", event.GetRepo().GetName(),
						"workflow", wfRun.GetWorkflowID(),
						"number", wfRun.GetRunNumber(),
						"id", wfRun.GetID(),
						"event", wfRun.GetEvent(),
						"status", wfRun.GetStatus(),
						"conclusion", wfRun.GetConclusion(),
						"actor", wfRun.GetActor().GetLogin(),
						"created_at", wfRun.GetCreatedAt().Unix(),
						"updated_at", wfRun.GetUpdatedAt().Unix(),
						"started_at", wfRun.GetRunStartedAt().Unix(),
					)

					if err := db.StoreWorkflowRunEvent(event); err != nil {
						health.Set(exporter.WorkflowRunHealthDatabaseError, true)
						logger.Error("Failed to store github event",
							"type", "workflow_run",
							"owner", event.GetRepo().GetOwner().GetLogin(),
							"repo", event.GetRepo().GetName(),
							"workflow", wfRun.GetWorkflowID(),
							"number", wfRun.GetRunNumber(),
							"error", err,
						)

						w.Header().Set("Content-Type", "text/plain")
						w.WriteHeader(http.StatusInternalServerError)

						_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
						return
					}
					if cfg.Collector.WorkflowRuns {
						if err := db.StoreWorkflowRunAttemptEvent(event); err != nil {
							health.Set(exporter.WorkflowRunHealthDatabaseError, true)
							logger.Error("Failed to store workflow run attempt",
								"owner", event.GetRepo().GetOwner().GetLogin(),
								"repo", event.GetRepo().GetName(),
								"workflow", wfRun.GetWorkflowID(),
								"number", wfRun.GetRunNumber(),
								"error", err,
							)

							w.Header().Set("Content-Type", "text/plain")
							w.WriteHeader(http.StatusInternalServerError)

							_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
							return
						}
					}
				case *github.WorkflowJobEvent:
					wfJob := event.GetWorkflowJob()
					logger.Debug("received webhook request",
						"type", "workflow_job",
						"owner", event.GetRepo().GetOwner().GetLogin(),
						"repo", event.GetRepo().GetName(),
						"id", wfJob.GetID(),
						"name", wfJob.GetName(),
						"attempt", wfJob.GetRunAttempt(),
						"status", wfJob.GetStatus(),
						"conclusion", wfJob.GetConclusion(),
						"created_at", wfJob.GetCreatedAt().Unix(),
						"started_at", wfJob.GetStartedAt().Unix(),
						"completed_at", wfJob.GetCompletedAt().Unix(),
						"labels", strings.Join(wfJob.Labels, ", "),
					)

					if err := db.StoreWorkflowJobEvent(event); err != nil {
						logger.Error(
							"failed to store github event",
							"type", "workflow_job",
							"owner", event.GetRepo().GetOwner().GetLogin(),
							"repo", event.GetRepo().GetName(),
							"name", wfJob.GetName(),
							"id", wfJob.GetID(),
							"error", err,
						)

						w.Header().Set("Content-Type", "text/plain")
						w.WriteHeader(http.StatusInternalServerError)

						_, _ = io.WriteString(w, http.StatusText(http.StatusInternalServerError))
						return
					}
				}

				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)

				_, _ = io.WriteString(w, http.StatusText(http.StatusOK))
			})
		}

		root.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)

			_, _ = io.WriteString(w, http.StatusText(http.StatusOK))
		})

		root.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)

			_, _ = io.WriteString(w, http.StatusText(http.StatusOK))
		})
	})

	return mux
}
