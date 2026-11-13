# Third-party components

This repository contains experimental adaptations of HGMatch, MBFT and vChain+. Their names identify baseline methods; these adaptations are not presented as the original authors' complete implementations.

- vChain+ source is included under `VChainBaseline/vchain-plus` and `experiments/defi_native_verification/revision_v4/vchain-plus`. The accompanying `LICENSE` files apply to those components.
- The MBFT source trees include `nlohmann/json` and `cmdline`. Their original copyright and license notices are retained in the header files. The surrounding MBFT adaptation does not acquire a new license merely by being packaged with those headers.
- HGMatch adapters extend the event-matching interface with the role, time and canonical-result semantics required by the workloads.
- Rust registry dependencies are resolved through the included Cargo manifests and lockfiles. They are not vendored into this release.

No repository-wide license has been added. Reuse and redistribution remain subject to the rights and licenses of each component.
