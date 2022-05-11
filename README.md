# Columbia
An efficient database query optimizer for large complex join queries

New interest in query capabilities for on-line analytical processing (OLAP) and large data warehouses created new challenge to current optimizers in commercial relational database, which have often proved inadequate to the needs of these application areas. Query optimizers were already among the largest and most complex modules of database systems, and they have proven difficult to modify and extend to accommodate these areas. 

Columbia query optimizer framework provided an effective test environment for aggregation transforms on decision-support queries. This is the source code for the Columbia query optimizer that I wrote during my Master thesis research in Portland State University.

Columbia Query Optimizer Project page: http://web.cecs.pdx.edu/~len/Columbia/


* query

(EQJOIN(<D.A>,<E.A>),
    (EQJOIN(<C.Z>,<D.Z>),
        (EQJOIN(<AB.Y>,<C.Y>),
            (GET(AB)),
            (GET(C))),
        (GET(D))),
    (GET(E)))


group:0 EQJOIN(<D.A>,<E.A>), input: 1, input: 6
group:1 EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5
group:2 EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4
group:3 GET(AB)
group:4 GET(C)
group:5 GET(D)
group:6 GET(E)


OptimizeGroupTask
優化group中的所有表達式，對表達式應用OptimizeExprTask規則
task: 0   OptimizeExprTask group EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577

OptimizeExprTask
優化表達式，使用rule對表達式進行轉換，其表達式的組織方式為mexpression，輸入是group，
在使用ApplyRuleTask之後，依據rule的Pattern的input來決定是否需要優化input  所以引用規則之前需要先對輸入的group應用ExploreGroupTask
    task: 0   ApplyRuleTask rule: RULE EXCHANGE, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 1   ExploreGroupTask group 6, parent task 0
    task: 2   ExploreGroupTask group 1, parent task 0
    task: 3   ApplyRuleTask rule: RULE EQJOIN_RTOL, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 4   ExploreGroupTask group 6, parent task 0
    task: 5   ApplyRuleTask rule: RULE EQJOIN_COMMUTE, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 6   ApplyRuleTask rule: RULE EQJOIN_LTOR, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 7   ExploreGroupTask group 1, parent task 0
    task: 8   ApplyRuleTask rule: RULE EQJOIN -> BIT_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 9   ExploreGroupTask group 6, parent task 0
    task: 10   ApplyRuleTask rule: RULE EQJOIN->LOOPS_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 11   ApplyRuleTask rule: RULE EQJOIN -> LOOPS_INDEX_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 12   ApplyRuleTask rule: RULE EQJOIN->HASH_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 13   ApplyRuleTask rule: RULE EQJOIN -> MERGE_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577

ApplyRuleTask
判斷是否可以應用規則，如果可以應用規則生成新的表達式，
如果是transformation，則對新的表達式使用OptimizeExprTask
如果是implementation，則對新的表達式使用O_INPUTS
    task: 10   ApplyRuleTask rule: RULE EQJOIN->LOOPS_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 11   ApplyRuleTask rule: RULE EQJOIN -> LOOPS_INDEX_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 12   ApplyRuleTask rule: RULE EQJOIN->HASH_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 13   O_INPUTS expression: MERGE_JOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 0, Prop: Any Prop, UB: -1.000000

O_INPUTS
對應的是implementation，如果表達式有輸入，則在處理完表達式之前，繼續保持當前規則壓棧，遞歸的處理input，對input使用OptimizeGroupTask，
且按照當前表達式的需求把Prop作爲enforcer rule傳遞給input
下面是完整的一個優化片段

    task: 43   ApplyRuleTask rule: RULE EQJOIN -> MERGE_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 1935766560

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   OptimizeGroupTask group: 3, parent task: 0 Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   OptimizeExprTask group GET(AB), parent task 0

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   ApplyRuleTask rule: RULE Get->FileScan, mexpr GET(AB), parent task 0

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   O_INPUTS expression: FILE_SCAN(AB), parent task 0, Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 3, parent task: 0 Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(AB), parent task 0, Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   ApplyRuleTask rule: RULE SORT enforcer, mexpr GET(AB), parent task 0

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(AB), parent task 0, Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   O_INPUTS expression: QSORT, input: 3, parent task 538970672, Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(AB), parent task 0, Prop: sorted on (AB.Y)  KeyOrder: (ascending, UB: 31.776246

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   OptimizeGroupTask group: 4, parent task: 32577 Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   OptimizeExprTask group GET(C), parent task 0

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   ApplyRuleTask rule: RULE Get->FileScan, mexpr GET(C), parent task 0

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   O_INPUTS expression: FILE_SCAN(C), parent task 0, Prop: Any Prop, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   OptimizeGroupTask group: 4, parent task: 0 Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(C), parent task 0, Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   ApplyRuleTask rule: RULE SORT enforcer, mexpr GET(C), parent task 32577

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(C), parent task 0, Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000
    task: 45   O_INPUTS expression: QSORT, input: 4, parent task 1935766560, Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: -1.000000

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 44   O_INPUTS expression: FILE_SCAN(C), parent task 0, Prop: sorted on (C.Y)  KeyOrder: (ascending, UB: 1.916494

    task: 43   O_INPUTS expression: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0, Prop: Any Prop, UB: -1.000000


group:0 EQJOIN(<D.A>,<E.A>), input: 1, input: 6
group:1 EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5
group:2 EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4
group:3 GET(AB)
group:4 GET(C)
group:5 GET(D)
group:6 GET(E)

    task: 0   ApplyRuleTask rule: RULE EXCHANGE, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 1   ExploreGroupTask group 6, parent task 0
    task: 2   ExploreGroupTask group 1, parent task 0
    task: 3   ApplyRuleTask rule: RULE EQJOIN_RTOL, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 4   ExploreGroupTask group 6, parent task 0
    task: 5   ApplyRuleTask rule: RULE EQJOIN_COMMUTE, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 6   ApplyRuleTask rule: RULE EQJOIN_LTOR, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 7   ExploreGroupTask group 1, parent task 0
    task: 8   ApplyRuleTask rule: RULE EQJOIN -> BIT_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 9   ExploreGroupTask group 6, parent task 0
    task: 10   ApplyRuleTask rule: RULE EQJOIN->LOOPS_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 11   ApplyRuleTask rule: RULE EQJOIN -> LOOPS_INDEX_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 12   ApplyRuleTask rule: RULE EQJOIN->HASH_JOIN, mexpr EQJOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 32577
    task: 13   O_INPUTS expression: MERGE_JOIN(<D.A>,<E.A>), input: 1, input: 6, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 14   OptimizeGroupTask group: 1, parent task: 0 Prop: sorted on (D.A)  KeyOrder: (ascending, UB: -1.000000
    task: 15   ApplyRuleTask rule: RULE EXCHANGE, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0
    task: 16   ExploreGroupTask group 5, parent task 0
    task: 17   ExploreGroupTask group 2, parent task 0
    task: 18   ApplyRuleTask rule: RULE EQJOIN_RTOL, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0
    task: 19   ExploreGroupTask group 5, parent task 0
    task: 20   ApplyRuleTask rule: RULE EQJOIN_COMMUTE, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0
    task: 21   ApplyRuleTask rule: RULE EQJOIN_LTOR, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0
    task: 22   ExploreGroupTask group 2, parent task 0
    task: 23   ApplyRuleTask rule: RULE EQJOIN -> BIT_JOIN, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0
    task: 24   ExploreGroupTask group 5, parent task 0
    task: 25   ApplyRuleTask rule: RULE EQJOIN->LOOPS_JOIN, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 539767072
    task: 26   ApplyRuleTask rule: RULE EQJOIN -> LOOPS_INDEX_JOIN, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 1280267346
    task: 27   ApplyRuleTask rule: RULE EQJOIN->HASH_JOIN, mexpr EQJOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 892482336
    task: 28   O_INPUTS expression: MERGE_JOIN(<C.Z>,<D.Z>), input: 2, input: 5, parent task 0, Prop: Any Prop, UB: -1.000000
    task: 29   OptimizeGroupTask group: 2, parent task: 0 Prop: sorted on (C.Z)  KeyOrder: (ascending, UB: -1.000000
    task: 30   ApplyRuleTask rule: RULE EXCHANGE, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0
    task: 31   ExploreGroupTask group 4, parent task 0
    task: 32   ExploreGroupTask group 3, parent task 0
    task: 33   ApplyRuleTask rule: RULE EQJOIN_RTOL, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0
    task: 34   ExploreGroupTask group 4, parent task 0
    task: 35   ApplyRuleTask rule: RULE EQJOIN_COMMUTE, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0
    task: 36   ApplyRuleTask rule: RULE EQJOIN_LTOR, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0
    task: 37   ExploreGroupTask group 3, parent task 0
    task: 38   ApplyRuleTask rule: RULE EQJOIN -> BIT_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 0
    task: 39   ExploreGroupTask group 4, parent task 0
    task: 40   ApplyRuleTask rule: RULE EQJOIN->LOOPS_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 539767072
    task: 41   ApplyRuleTask rule: RULE EQJOIN -> LOOPS_INDEX_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 1280267346
    task: 42   ApplyRuleTask rule: RULE EQJOIN->HASH_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 892482336
    task: 43   ApplyRuleTask rule: RULE EQJOIN -> MERGE_JOIN, mexpr EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4, parent task 1935766560

Group: 2
        Logic M_Expr: EQJOIN(<AB.Y>,<C.Y>), input: 3, input: 4;
        Logic M_Expr: EQJOIN(<C.Y>,<AB.Y>), input: 4, input: 3;
        Physc M_Expr: MERGE_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4;
        Physc M_Expr: HASH_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4;
        Physc M_Expr: LOOPS_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4;
        Physc M_Expr: MERGE_JOIN(<C.Y>,<AB.Y>), input: 4, input: 3;
        Physc M_Expr: HASH_JOIN(<C.Y>,<AB.Y>), input: 4, input: 3;
        Physc M_Expr: LOOPS_JOIN(<C.Y>,<AB.Y>), input: 4, input: 3;
        Physc M_Expr: QSORT, input: 2;
        ----- has 9 MExprs -----
Winners:
        Any Prop, HASH_JOIN(<AB.Y>,<C.Y>), input: 3, input: 4, 9.255590, Done
        sorted on (C.Z)  KeyOrder: (ascending, HASH_JOIN(<C.Y>,<AB.Y>), input: 4, input: 3, 10.819944, Not done
LowerBound: 0.388580
log_prop:   Card: 26846.000000  UCard: 26846.000000


-----
表达式的实现
1. 表达式和功能分离，表达式只是存粹的表达式，不包含其他的额外功能，
  SQL可以抽象为某几个阶段，表达式从一个阶段进入到下一个阶段，利于实现。层次分明，代码解耦，易于测试
  ```c++
    ast = parser(sql)
    {
      bind(ast);
      optmizer(ast);
      execute(ast);
      break;
    }
  ```
2. 表达式和功能结合，表达式实现不同功能的实现方法，表达式始终存在，SQL执行到某阶段。只是执行到表达式的某阶段的代码实现
  紧耦合，SQL的生命周期可以抽象为一个对象的函数的执行，最终表达式会膨胀到难以理解的地步，不利于测试。
  ```c++
    statement = parser(sql)
    {
      statement.bind();
      statement.optmizer();
      statement.execute();
    }
  ```
3. 不同阶段使用不同的实现，parser binder按照statement为单位表示SQL，内部可以有自己的表示方法，
  ```c++
    statement = parser(sql)
    {
      statement.bind();
      optree = statement.convert;
      optmizer(optree);
      execute(optree);
    }
  ```

parser 
binder
optimzer
executor


