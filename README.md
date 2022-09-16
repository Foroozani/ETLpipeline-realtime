## AWS Step Functions: the Deployment Orchestrator that CodePipeline should have been

AWS Step Functions is a low-code, visual workflow service that developers use to build distributed applications, automate IT and business processes, and build data and machine learning pipelines using AWS services. Workflows manage failures, retries, parallelization, service integrations, and observability so developers can focus on higher-value business logic.

## CodePipeline and Step Functions
In this article I want to mention three powerful features in a Step Functions pipeline that are hard or impossible to achieve in CodePipeline. The first is **parallel builds**, the second is **dynamic source** branches and the third is **conditional execution** branches. My personal approach is to use CodePipeline only for the most basic applications. As soon as the deployment mechanism consists of multiple components, requires dynamic branches or has a future in which any of these features might be required, I tend to choose Step Functions.

### 7 types of states
1. A [Tasks](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html) state ("Type": "Task") represents a single unit of work performed by a state machine. All work in your state machine is done by Task. A task performs work by using an activity or an AWS Lambda function, or by passing parameters to the API actions of other services.
2. A [Pass](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-pass-state.html) state ("Type": "Pass") passes its input to its output, without performing work. Pass states are useful when constructing and debugging state machines.
3. A [Wait](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-wait-state.html) state ("Type": "Wait") delays the state machine from continuing for a specified time. You can choose either a relative time, specified in seconds from when the state begins, or an absolute end time, specified as a timestamp.
4. A [Choice](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-choice-state.html) state ("Type": "Choice") adds branching logic to a state machine.
5. The [Parallel](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-parallel-state.html) state ("Type": "Parallel") can be used to create parallel branches of execution in your state machine.
6. A [Succeed](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-succeed-state.html) state ("Type": "Succeed") stops an execution successfully. The Succeed state is a useful target for Choice state branches that don't do anything but stop the execution.
7. A [Fail](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-fail-state.html) state ("Type": "Fail") stops the execution of the state machine and marks it as a failure, unless it is caught by a Catch block.

![stepFunctions-image](https://github.com/Foroozani/ETLpipeline-realtime/blob/main/figures/img3.png)
![stepFunctions-image](https://github.com/Foroozani/ETLpipeline-realtime/blob/main/figures/img2.png)
