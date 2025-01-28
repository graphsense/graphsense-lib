from graphsenselib.utils.pipeline import Pipeline, StopPipeline, set_context
from graphsenselib.utils.pipeline import Process as P


def inc(ctx, item):
    return item + 1


def double(ctx, item):
    return item * 2


def add(ctx, item):
    return item + ctx.b


def raise_ex(ctx, item):
    raise ValueError("ohhh noooo")

    # s = [
    #     P(
    #         task=print_and_add_i,
    #         name="should be 11",
    #         init_func=set_context,
    #         init_args=({"b": 10},),
    #     )
    #     .pipe(
    #         P(
    #             task=print_and_add_i,
    #             name="should be 21",
    #             init_func=set_add,
    #             init_args=(10,),
    #         )
    #     )
    #     .pipe(
    #         P(
    #             task=print_and_add_i,
    #             name="should be 111",
    #             init_func=set_add,
    #             init_args=(100,),
    #         )
    #     ),
    #     P(task=print_and_add, name="should be 2").pipe(
    #         P(task=print_and_mult, name="should be 2")
    #     ),
    # ]


def test_simple_pipeline():
    s = [
        P(task=add, init_func=set_context, init_args=({"b": 10},))
        | P(task=add, init_func=set_context, init_args=({"b": 10},))
    ]

    with Pipeline(s) as p:
        p.put(1)
        p.put(100)
        p.put(StopPipeline())

        p.join()

        res = p.get_results()

    assert res == [21, 120]


def test_simple_pipeline_with_exeption1():
    s = [
        P(task=add, name="adder", init_func=set_context, init_args=({"b": 10},))
        | P(task=raise_ex, name="raiser")
    ]

    with Pipeline(s) as p:
        p.put(1)
        p.put(StopPipeline())

        p.join()

        res = p.get_results()

    assert len(res) == 1
    assert str(res[0].exc) == "ohhh noooo"
    assert isinstance(res[0].exc, ValueError)


def test_simple_pipeline_with_exeption2():
    # exceptions are passed through and terminate workers
    s = [
        P(task=raise_ex, name="raiser")
        | P(task=add, name="adder", init_func=set_context, init_args=({"b": 10},))
    ]

    with Pipeline(s) as p:
        p.put(1)
        p.put(100)
        p.put(StopPipeline())
        p.put(StopPipeline())

        p.join()

        res = p.get_results()

    assert len(res) == 1
    assert str(res[0].exc) == "ohhh noooo"
    assert isinstance(res[0].exc, ValueError)


def test_pipeline_with_downstream_exeption_and_no_stop():
    s = [
        P(task=add, name="adder", init_func=set_context, init_args=({"b": 10},))
        | P(task=raise_ex, name="raiser")
    ]

    with Pipeline(s) as p:
        p.put(1)
        # p.put(StopPipeline())

        p.join()

        res = p.get_results()

    assert len(res) == 1
    assert str(res[0].exc) == "ohhh noooo"
    assert isinstance(res[0].exc, ValueError)


# def test_pipeline_with_downstream_exeption_and_no_stop():
#     s = [
#         P(task=add, name="adder", init_func=set_context, init_args=({"b": 10},))
#         | P(task=raise_ex, name="raiser")
#     ]

#     with Pipeline(s) as p:
#         p.put(1)
#         # p.put(StopPipeline())

#         p.join()

#         res = p.get_results()

#     assert len(res) == 1
#     assert str(res[0].exc) == "ohhh noooo"
#     assert type(res[0].exc) == ValueError
