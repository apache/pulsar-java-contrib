# Apache Pulsar Java Contrib

Pulsar java contrib is to provide a non-core code maintenance repository to collect plugin implementations, personalized features, experimental features, and best practices from users.

- [Plugin Contribution Guide](conrtibutionGuides.md) lists the core interfaces in Pulsar that can be implemented by contributors, and provides implementation guidelines for each type of interface.

- [Plugin Implementation List](contributedFeatures.md) lists the implemented plugins. Users can select the ones they need for reuse.

- [Personalization Features](customizationFeatures.md) lists the customized features and experimental features that require modification to the Pulsar source code.

- [Best Practices]([best-pratice-blogs](best-pratice-blogs)) lists the best practices for each function summarized by community contributions.
  - [consume-best-practice.md](best-pratice-blogs%2Fconsume-best-practice.md)

This project follows the terms of **Apache License 2.0**.
You can format the code by ` mvn spotless:apply` and generate license headers by `mvn license:format`.
Please note that the code formatted by Spotless may still not meet the formatting requirements. Please run `mvn checkstyle:check` for inspection.

## Contributing

pulsar-java-contrib is actively in development.  If you have some common use cases for plugins, please contact us and we'll be happy to support.
Please[open an issue](https://github.com/StevenLuMT/pulsar-java-contrib/issues/new/choose) to share your idea or
suggestion.  PRs are always welcome and greatly appreciated, but for larger functional changes a pre-coding introduction
can be helpful to ensure this is the correct place and that active or conflicting efforts don't exist.

Emeritus maintainers:
- [Steven Lu](https://github.com/StevenLuMT)
- [Liangyepian Zhou](https://github.com/liangyepianzhou)
- [Linlin Duan](https://github.com/AuroraTwinkle)
- [Fenggan Cai](https://github.com/cai152)
- [Jia Zhai](https://github.com/jiazhai)

Learn more about roles in the [community repository](https://github.com/StevenLuMT/pulsar-java-contrib).
