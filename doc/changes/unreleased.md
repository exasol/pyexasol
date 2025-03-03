# Unreleased

## ✨Features

* Added support for multi-version documentation
* Added support for all standard nox tasks provided by `exasol-toolbox`

## ⚒️ Refactorings

* Reformated entire code base with `black` and `isort`

## 🔩 Internal

* Relocked dependencies
* Added exasol-toolbox workflows and actions
* Added missing plugin for multi-version documentation
* Added support for publishing documentation to gh pages
* Added `.git-blame-ignore-revs` file to workspace

    Note: please make sure to adjust yor git config accordingly (if not done yet)

        ```shell
        git config blame.ignoreRevsFile .git-blame-ignore-revs
        ```

## 📚 Documentation

* Added sphinx based documentation
* Added example to highlight how sensitive information from exceptions should be handled 

