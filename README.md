# tibber-pyAws
AWS related packages.
Built using GitHub Actions and releases created from GitHub and pushed to pypi.

## Packages
* [tibber_aws](https://pypi.org/project/tibber-aws/)

## Create Release

### CLI
* Update version in setup.py
* `gh release create <tag> -t <tag>`
Example
```sh
gh release create 0.8.6 -t 0.8.6
```

### Manually
* Update version in setup.py
* Create a [Release](https://github.com/tibbercom/tibber-pyAws/releases)
  This will push the package to pypi.
