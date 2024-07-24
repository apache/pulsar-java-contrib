# Apache Pulsar Java Contrib

[![Build](https://github.com/StevenLuMT/pulsar-java-contrib/actions/workflows/build.yml/badge.svg)](https://github.com/StevenLuMT/pulsar-java-contrib/actions/workflows/build.yml)

pulsar-java-contrib is similar to the positioning of [opentelemetry-java-contrib](https://github.com/open-telemetry/opentelemetry-java-contrib): 
* One is to prevent the Pulsar main repository from adding too many unnecessary functions, which would make Pulsar too bloated, increasing the user's usage cost and our maintenance cost.
* The other is to reduce the user's usage cost by implementing a ready-to-use implementation class based on Pulsar's external interface.

these ways achieve these two goals are the plugin library and the yellow pages. If you need an easier way to implement some plugin library based on Pulsar that cannot be easily satisfied by importing Pulsar directly, then this project is hopefully for you.

## Provided Libraries


* [Pulsar Client Contrib](./pulsar-client-contrib/README.md)

## Getting Started

```bash

```

## Contributing

pulsar-java-contrib is actively in development.  If you have an idea for a similar use case in the metrics, traces, or logging
domain we would be very interested in supporting it.  Please
[open an issue](https://github.com/StevenLuMT/pulsar-java-contrib/issues/new/choose) to share your idea or
suggestion.  PRs are always welcome and greatly appreciated, but for larger functional changes a pre-coding introduction
can be helpful to ensure this is the correct place and that active or conflicting efforts don't exist.

Emeritus maintainers:
- [Steven Lu](https://github.com/StevenLuMT)
- [Liangyepian Zhou](https://github.com/liangyepianzhou)
- [Linlin Duan](https://github.com/AuroraTwinkle)
- [Fenggan Cai](https://github.com/cai152)
- [Jia Zhai](https://github.com/jiazhai)

Learn more about roles in the [community repository](https://github.com/StevenLuMT/pulsar-java-contrib).
