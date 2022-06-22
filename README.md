# Step Functions 

## 7 types of states
1. A [Tasks](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html) state ("Type": "Task") represents a single unit of work performed by a state machine. All work in your state machine is done by Task. A task performs work by using an activity or an AWS Lambda function, or by passing parameters to the API actions of other services.
2. A [Pass](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-pass-state.html) state ("Type": "Pass") passes its input to its output, without performing work. Pass states are useful when constructing and debugging state machines.
3. A [Wait](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-wait-state.html) state ("Type": "Wait") delays the state machine from continuing for a specified time. You can choose either a relative time, specified in seconds from when the state begins, or an absolute end time, specified as a timestamp.
4. A [Choice](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-choice-state.html) state ("Type": "Choice") adds branching logic to a state machine.
5. The [Parallel](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-parallel-state.html) state ("Type": "Parallel") can be used to create parallel branches of execution in your state machine.
6. A [Succeed](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-succeed-state.html) state ("Type": "Succeed") stops an execution successfully. The Succeed state is a useful target for Choice state branches that don't do anything but stop the execution.
7. A [Fail](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-fail-state.html) state ("Type": "Fail") stops the execution of the state machine and marks it as a failure, unless it is caught by a Catch block.

![CNN-image](https://github.com/Foroozani/ETLpipeline-realtime/blob/main/figures/img1.png)
