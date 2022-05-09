# Contributing to Zef

Thank you for expressing interest in contributing to Zef! Every contribution pushes Zef forward to make it better for current and future users.

We're a small team maintaining Zef and at the moment, want to minimize process.

## Where you can contribute

You can contribute across all fronts, from improving the documentation, submitting bug reports and feature requests, or writing code which can be incorporated into Zef.

## How to contribute

We currently do not have a strict process or guidelines. To optimize for feedback speed, please raise bugs, suggest features, or ask if anything seems unclear, missing, or broken directly in our community chat at [https://zef.chat](https://zef.chat).

Please don't be shy on submitting a pull request - just dive in!

## Your first contribution

A good first place to start contributing is the documentation and tutorials. We feel this is a good way for people who want to get involved to understand Zef and start making meaningful contributions right away!

### Setting up git pre-commit hook

If you are using VS Code you have to un-hide the `.git` folder, this can be done by creating the following config file.

`.vscode/settings.json`
```json
{
    "files.exclude": {
        "**/.git": false
   }
}
```

The next step is creating the pre-commit file. Notice how the file name has no extention.

`.git/hooks/pre-commit`
```sh
#!/bin/bash

# If there are license errors, print the offending file names and fail.
exec .github/scripts/check-license.sh 2>&1
```

Next we need to enable execution.
```sh
chmod +x .git/hooks/pre-commit
```

Finally we need to install the `addlicense` tool, please note that you need to have [Go Lang]([https://go.dev/) installed on your system.

```sh
go install github.com/zefhub/addlicense@18fa4120a13e50674b12c3d36748fc7d84596dad
```

Now you are ready to go, the hook should run automaticly before every commit.
