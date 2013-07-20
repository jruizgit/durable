require("../lib/durable");

durable.run({

    sequence: durable.receive({ content: "hello" })
        .continueWith(function (s) { console.log(s.getOutput().content) })
        .receive({ content: "world" })
        .continueWith(function (s) { console.log(s.getOutput().content) })
        .checkpoint("stage 1")
        .receive({ content: "good" })
        .continueWith(function (s) { console.log(s.getOutput().content) })
        .receive({ content: "bye" })
        .continueWith(function (s) { console.log(s.getOutput().content) })
        .checkpoint("stage 2"),

    algebra: durable.waitAnyEvent({
            a: durable.tryReceive({ content: "go" }),
            b: durable.tryReceive({ content: "jump" }),
            c$and: {
                a: durable.tryReceive({ content: "move" }),
                b: durable.tryReceive({ content: "next" })
            }
        })
        .continueWith(function (s) {
            console.log("Event Fired");
            if (s.getOutput().a) {
                console.log(s.getOutput().a.content);
            }
            else if (s.getOutput().b) {
                console.log(s.getOutput().b.content);
            }
            else {
                console.log(s.getOutput().c.a.content);
                console.log(s.getOutput().c.b.content);
            }
        })
        .checkpoint("exit"),

    dynamic: durable.promise(function (s) {   
            s.setInput({
                a: { query: { content: "go" } },
                b: { query: { content: "jump" } },
                c: {
                    a: { query: { content: "move" } },
                    b: { query: { content: "next" } }
                }
            });
        })
        .waitAnyEvent({
            a: durable.tryReceive(),
            b: durable.tryReceive(),
            c$and: {
                a: durable.tryReceive(),
                b: durable.tryReceive()
            }
        })
        .continueWith(function (s) {
            console.log("Event Fired");
            if (s.getOutput().a) {
                console.log(s.getOutput().a.content);
            }
            else if (s.getOutput().b) {
                console.log(s.getOutput().b.content);
            }
            else {
                console.log(s.getOutput().c.a.content);
                console.log(s.getOutput().c.b.content);
            }
        })
        .checkpoint("exit"),

    delay: durable.receive({ content: "start" })
        .continueWith(function (s) { console.log("start:" + new Date().toUTCString()) })
        .delay(30, "firstDelay")
        .continueWith(function (s) { console.log("first:" + new Date().toUTCString()) })
        .startTimer(30, "secondDelay")
        .checkpoint("second")
        .waitAnyEvent({
            a: durable.tryReceive({ content: "end" }),
            b: durable.tryTimer("secondDelay")
        })
        .continueWith(function (s) {
            if (s.getOutput().a) {
                console.log("end:" + s.getOutput().a.content);
            }
            else {
                console.log("timeout:" + new Date().toUTCString());
            }
        }),

    allstreams: durable.waitAllStreams({
            first: durable.promise(function (s) { console.log("First branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue first branch");
                        s.first = s.getOutput().content;
                    })
                    .checkpoint("firstStream"),
            second: durable.promise(function (s) { console.log("Second branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue second branch");
                        s.second = s.getOutput().content;
                    })
                    .checkpoint("secondStream"),
            third: durable.promise(function (s) { console.log("Third branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue third branch");
                        s.third = s.getOutput().content;
                    })
                    .checkpoint("thirdStream")
        }, "allStreams")
        .continueWith(function (s) {
            console.log("All branches complete");
            console.log(s.first);
            console.log(s.second);
            console.log(s.third);
        })
        .checkpoint("exit"),

    anystream: durable.waitAnyStream({
            first: durable.promise(function (s) { console.log("First branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue first branch");
                        s.first = s.getOutput().content;
                    })
                    .checkpoint("firstStream"),
            second: durable.promise(function (s) { console.log("Second branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue second branch");
                        s.second = s.getOutput().content;
                    })
                    .checkpoint("secondStream"),
            third: durable.promise(function (s) { console.log("Third branch started"); })
                    .receive({})
                    .continueWith(function (s) {
                        console.log("Continue third branch");
                        s.third = s.getOutput().content;
                    })
                    .checkpoint("thirdStream")
        }, "anyStream")
        .continueWith(function (s) {
            console.log("One branch complete");
            if (s.first) {
                console.log(s.first);
            }

            if (s.second) {
                console.log(s.second);
            }

            if (s.third) {
                console.log(s.third);
            }
        })
        .checkpoint("exit"),

    mapreduce: durable.receive({ content: { $regex: "[0-9]*" } })
        .mapReduce(function(s) { 
                number = s.getOutput();
                mapss = [];
                for (index = 0; index < number.content; ++index)
                {
                    mapss[index] = { content: index };
                }
                console.log("Map ss :" + mapss.length);        
                return mapss;
            },
            durable.receive({})
            .continueWith(function(s) { console.log("Map: " + s.content) }),
            function(ss) {
                var total = 0;        
                for (index = 0; index < ss.length; ++index) {
                    total = total + ss[index].content;
                }
                return { content: total };
            }, "map")
        .continueWith(function (s) { console.log("Reduce: " + s.content); }),


    statemachine: durable.stateChart({
        state0: {
            firstTransition: {
                whenAny: {
                    a$and: {
                        a: durable.tryReceive({ content: "a" }),
                        b: durable.tryReceive({ content: "b" })
                    },
                    b$and: {
                        c: durable.tryReceive({ content: "c" }),
                        d: durable.tryReceive({ content: "d" }),
                    }
                },
                run: function (s) { console.log("State0 First Transition") },
                to: "state1"
            },
            secondTransition: {
                when: durable.tryReceive({ content: "e" }),
                run: function (s) { console.log("State0 Second Transition") },
                to: "state2"
            }
        },
        state1: {
            back0Transition: {
                when: durable.tryReceive({ content: "f" }),
                run: function (s) { console.log("State1 back Transition") },
                to: "state0"
            },
            finalTransition: {
                when: durable.tryReceive({ content: "g" }),
                run: function (s) { console.log("State1 Final Transition") },
                to: "final"
            }
        },
        state2: {
            back1Transition: {
                when: durable.tryReceive({ content: "h" }),
                run: function (s) { console.log("State2 back Transition") },
                to: "state1"
            },
            final2Transition: {
                when: durable.tryReceive({ content: "i" }),
                run: function (s) { console.log("State2 Final Transition") },
                to: "final"
            }
        },
        final: {
        }
    }),

    calculator: durable.stateChart({
        on: {
            $chart: {
                op1: {
                    operand: {
                        when: durable.tryReceive({ content: "add"  }),
                        run: function (s) { s.operand = '+'; },
                        to: "opentered"
                    }
                },
                opentered: {
                    number: {
                        when: durable.tryReceive({ content: { $regex: "[0-9]." } }),
                        run: function (s) { s.value = s.getOutput().content },
                        to: "op2"
                    }
                },
                op2: {
                    equal: {
                        when: durable.tryReceive({ content: "=" }),
                        run: function (s) {
                            if (!s.result) {
                                s.result = 0;
                            }

                            evalString = s.result + s.operand + s.value;                        
                            s.result = eval(evalString);
                            console.log(evalString + "=" + s.result);
                        },
                        to: "result"
                    }
                },
                result: {
                    number: {
                        when: durable.tryReceive({ content: { $regex: "[0-9]." } }),
                        run: function (s) { s.result = s.getOutput().content },
                        to: "op1"
                    },
                    operand: {
                        when: durable.tryReceive({ content: "add" }),
                        run: function (s) { s.operand = s.getOutput().content; },
                        to: "opentered"
                    }
                },
            },
            offTransition: {
                when: durable.tryReceive({ content: "off" }),
                run: function (s) { console.log("Off") },
                to: "off"
            },
            clearTransition: {
                when: durable.tryReceive({ content: "c" }),
                run: function (s) { console.log("Clear"); s.result = 0; },
                to: "on"
            }
        },
        off: {        
        }
    }),

    flow: durable.flowChart({
        stage0: {
            run: durable.waitAllEvents({
                    a: durable.tryReceive({ content: "a" }),
                    b: durable.tryReceive({ content: "b" })
                })
                .continueWith(function (s) { console.log("stage0 to " + s.getOutput().b.content) }),
            to: "stage1"
        },
        stage1: {
            run: durable.receive({
                    $or: [{ content: "c" },
                          { content: "d" },
                          { content: "e" }]
                })
                .continueWith(function (s) {
                     console.log("stage1 to " + s.getOutput().content);
                     s.setOutput(s.getOutput());
                }),
            to: {
                stage0: function (s) { return s.getOutput().content === "c" },
                stage2: function (s) { return s.getOutput().content === "d" },
                final: function (s) { return s.getOutput().content === "e" },
            }
        },
        stage2: {
            run: durable.receive({ content: "f" })
                 .continueWith(function (s) { console.log("stage2 to " + s.getOutput().content) }),
            to: "stage1"
        },
        final: {
        }
    })

});