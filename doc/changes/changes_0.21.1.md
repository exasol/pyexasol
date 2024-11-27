# 0.21.1 - 2021-09-27

- "HTTP Transport" and "Script Output" subprocess will now restore default handler for SIGTERM signal.

In some cases custom signal handlers can be inherited from parent process, which causes unwanted side effects and prevents correct termination of child process.

