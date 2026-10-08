#!/bin/sh
set -eu

# This local gate avoids printing matches, including if a real credential is
# accidentally added. It is a credential-pattern check, not a professional audit.
pattern='-----BEGIN (RSA|OPENSSH|EC|DSA|PRIVATE) KEY-----|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,}|sk-[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,}|xox[baprs]-[0-9A-Za-z-]{20,}|(postgres(ql)?|https?)://[^/[:space:]:@]+:[^@[:space:]/]+@'
if rg -q -i --hidden --pcre2 -g '!.git/**' -g '*.go' -g '*.md' -g '*.sh' -g '*.py' -g '*.json' -g '*.sql' -g '*.yml' -g '*.yaml' -g '*.toml' -g '.env.example' -g 'Dockerfile' -- "$pattern" .; then
    echo 'Potential credential-like material found; review locally without printing it.' >&2
    exit 1
fi
echo 'Local credential-pattern scan passed.'
