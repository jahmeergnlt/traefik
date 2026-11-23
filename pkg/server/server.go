races on read
	routers := make(map[string]RouterConfig, len(s.currentConfig.Routers))
	for k, v := range s.currentConfig.Routers {
		routers[k] = v
	}
	middlewares := make(map[string]MiddlewareConfig, len(s.currentConfig.Middlewares))
	for k, v := range s.currentConfig.Middlewares {
		middlewares[k] = v
	}

	return Configuration{
		Routers:     routers,
		Middlewares: middlewares,
	}
}