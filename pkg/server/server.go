for _, routerCfg := range config.Routers {
    cfg := routerCfg
    var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(cfg.ResponseText))
    })

    // Wrap with middleware if configured, using the configuration snapshot
    if cfg.Middleware != "" {
        if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
            handler = s.buildMiddleware(mwCfg, handler)
        }
    }

    mux.Handle(cfg.Path, handler)
}