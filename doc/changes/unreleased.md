# Unreleased

## ✨Features

* Add support for multiversion documentation
* Add support for all standard nox tasks provided by `exasol-toolbox`

## ⚒️ Refactorings

* Reformat entire code base with `black` and `isort`

## 🔩 Internal

* Relock dependencies
* Add exasol-toolbox workflows and actions
* Add missing plugin for multiversion documentation
* Add support for publishing documentation to gh pages
* Add `.git-blame-ignore-revs` file to workspace

    Note: please make sure to adjust yor git config accordingly (if not done yet)

        ```shell
        git config blame.ignoreRevsFile .git-blame-ignore-revs
        ```

## 📚 Documentation

* Added sphinx based documentation
* Added example to highlight how sensitive information from exceptions should be handled 

