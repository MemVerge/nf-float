# `nf-float`: Contributing Guidelines

Thank you for taking an interest in improving nf-float.

## Contribution workflow

If you'd like to write some code for nf-float, the standard workflow is as follows:

1. [Fork](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) the [nf-float repository](https://github.com/MemVerge/nf-float) to your GitHub account
2. Make the necessary changes / additions within your forked repository
4. Submit a Pull Request against the `master` branch and wait for the code to be reviewed and merged

If you're not used to this workflow with git, you can start with some [docs from GitHub](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests) or even their [excellent `git` resources](https://try.github.io/).

## Tests

You have the option to test your changes locally.

```bash
make test
```

When you create a pull request with changes, [GitHub Actions](https://github.com/features/actions) will run automatic tests.
Typically, pull-requests are only fully reviewed when these tests are passing, though of course we can help out before then.

## Release workflow

To make a new version of the plugin available on the Nextflow [plugin index](https://github.com/nextflow-io/plugins?tab=readme-ov-file), the standard workflow is as follows:

1. Bump the plugin version in `plugins/nf-float/src/resources/META-INF/MANIFEST.MF`
2. Follow the contribution workflow to get the updated `MANIFEST.MF` merged into the `master` branch
3. Create a tag following semantic versioning: `Major.Minor.Patch`
4. Push the tag to the upstream [nf-float repository](https://github.com/MemVerge/nf-float). This step needs write permissions.
5. Create a GitHub release based on the latest tag
6. Follow the official Package, upload, and publish [steps](https://github.com/nextflow-io/nf-hello?tab=readme-ov-file#package-upload-and-publish) for publishing the new plugin version
