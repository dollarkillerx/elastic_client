package simple_elasticsearch_client

type Options struct {
	schema     Schema
	auth       AUTH
	user       string
	password   string
	alloverTLS bool
}

func defaultOptions() *Options {
	return &Options{
		schema: HTTP,
		auth:   NoAuth,
	}
}

type SetOption = func(options *Options)

func SetSchema(schema Schema) SetOption {
	return func(options *Options) {
		options.schema = schema
	}
}

func SetPassword(user string, password string) SetOption {
	return func(options *Options) {
		options.user = user
		options.password = password
		options.auth = Passwd
	}
}

func AlloverTLS() SetOption {
	return func(options *Options) {
		options.alloverTLS = true
	}
}
