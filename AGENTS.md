# AGENTS.md

## Git 提交与推送

- 提交前先查看工作区状态：
  ```bash
  git --no-pager status --short --branch
  ```
- 提交前尽量运行与改动相关的验证命令。例如 `disruptor-lab` 项目：
  ```bash
  mvn -q -f code/disruptor-lab/pom.xml package
  ```
- 提交信息保持简短、清晰、使用祈使句，例如：
  ```text
  Add Disruptor lab demos
  ```
- 当前仓库推送应使用 `yitom486` 的 SSH alias，而不是默认 `git@github.com`，因为默认 SSH key 可能认证为其他 GitHub 用户。

### yitom486 SSH 推送方案

本机 SSH 配置中已有 alias：

```text
Host github-yitom486
    Hostname ssh.github.com
    Port 443
    User git
    IdentityFile ~/.ssh/id_ed25519_yitom486
```

将远端设置为：

```bash
git remote set-url origin github-yitom486:yitom486/ddia.git
```

可用下面命令确认认证身份：

```bash
ssh -T github-yitom486
```

期望看到：

```text
Hi yitom486! You've successfully authenticated, but GitHub does not provide shell access.
```

然后推送：

```bash
git push origin main
```

推送完成后确认状态：

```bash
git --no-pager status --short --branch
```

期望看到本地分支与 `origin/main` 同步，且工作区干净。
