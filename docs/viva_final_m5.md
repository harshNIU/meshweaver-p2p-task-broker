# Final Viva Preparation - Member 5 (M5)



## 1. What is MeshWeaver?

MeshWeaver is a Peer-to-Peer (P2P) Task Broker. It allows multiple computers (nodes) to communicate, discover one another, and distribute tasks for execution.



My responsibility is Member 5 (M5), focusing on CLI, dashboard integration, testing, quality assurance, and documentation.



## 2. What is the role of CLI?

CLI stands for Command-Line Interface. It allows users to interact with MeshWeaver by entering commands in the terminal.



MeshWeaver provides commands such as `start` and `submit`.



## 3. How do you start a node?

Use:



```powershell

python -m cli.main start

```



A node starts and listens for communication from other nodes.



To use a specific port:



```powershell

python -m cli.main start --port 8123

```



## 4. What is the purpose of the bootstrap option?

The bootstrap option helps a new node connect to an existing node in the network.



Example:



```powershell

python -m cli.main start --bootstrap 127.0.0.1:8000

```



It helps nodes discover other peers.



## 5. How do you submit a task?

The `submit` command sends a task request to a node.



Example:



```powershell

python -m cli.main submit add --args "[10,20]"

```



The task name is `add`, and the arguments are 10 and 20.



## 6. What is a dashboard?

A dashboard provides a visual summary of the system's current status.



It helps users monitor nodes and tasks without manually checking every operation.



## 7. What are the two dashboard options?

- **Rich terminal dashboard:** Displays node and task information in the terminal using Rich.

- **Web dashboard:** Provides a browser-based interface for viewing system state and events.



## 8. How do you start the web dashboard?

Use:



```powershell

python -m cli.main start --host 127.0.0.1 --port 8000 --web --web-port 8765

```



Then open:



`http://127.0.0.1:8765/`



## 9. What is the purpose of testing?

Testing checks whether the software works correctly and helps identify errors before delivery.



I contributed integration tests, CLI-related tests, and quality checks.



## 10. What is integration testing?

Integration testing checks whether different modules work correctly together.



For example, a test can start multiple nodes, submit a task, and verify that the expected result is received.



## 11. What is failover?

Failover means the system can recover when a node fails.



MeshWeaver's five-worker test verifies that tasks can still complete after two workers are stopped during execution.



## 12. What is a DHT?

DHT stands for Distributed Hash Table.



It helps nodes store and locate information in a distributed network without depending on one central server.



## 13. What is message signing?

Message signing helps verify that a message is authentic and has not been modified.



MeshWeaver uses HMAC-SHA256 for message authentication and integrity checking.



## 14. What is the purpose of pytest?

Pytest is a Python testing framework. It runs automated tests and reports whether they pass or fail.



Command:



```powershell

python -m pytest -q

```



## 15. What are Black and Ruff?

- **Black:** Checks Python code formatting.

- **Ruff:** Checks code for common errors and style problems.



They help maintain code quality.



## 16. What is Git?

Git is a version control system. It tracks changes in source code and helps team members collaborate.



## 17. What is GitHub?

GitHub is an online platform for hosting Git repositories and collaborating through branches and pull requests.



## 18. What is a pull request?

A pull request is a request to review and merge changes from one branch into another branch.



It allows teammates to review code before merging.



## 19. What was my contribution as M5?

My main contributions include:

- Developing and maintaining CLI-related functionality.

- Creating the Rich dashboard scaffold.

- Writing integration and failover tests.

- Testing communication and task execution.

- Maintaining documentation and viva preparation notes.

- Running pytest, Black, and Ruff checks.



## 20. What were the final test results?

The full test suite completed with **318 passed**.



Additional demonstrations completed:

- 10-node DHT audit: PASS.

- Five-worker, 20-task failover test: PASS.

- Routing demonstration: PASS.

- Message-signing and security tests: 25 passed.



These were separate test runs.



## 21. What is one limitation of the web dashboard?

The web dashboard's task submission handler executes through its local ExecutionEngine. A successful web submission alone does not prove remote peer execution.



Distributed task execution should be verified using the networking and routing tests.



## 22. What did I learn from this project?

I learned how to work with Python modules, command-line tools, distributed systems, automated testing, Git, GitHub, and team collaboration.



I also gained experience debugging issues and verifying that different modules work together.



## 23. How would you conclude your project presentation?

MeshWeaver demonstrates how tasks can be distributed among multiple peer nodes. As M5, I contributed to the CLI, dashboard, integration testing, quality assurance, and documentation. This project helped me understand practical software development and teamwork.
