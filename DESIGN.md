PlanNode は AccessMethod を掘っているべき
つまり HaveAccessMethod を継承している必要がある
それによって PlanNode は AccessMethod の関連型の SearchOption にも関連し
結果 AccessMessod の search にその SearchOption を関連させることができるはず

AccessMethod の search メソッドの search_option が SearchMode 固定にしたい。
1. btree 内の SearchMode の impl は btree 固有なので btree の使用箇所について開くなり関数化する
2. SearchMode を accessmethod の entity に移動させて btree からは参照にする
3. SearchOption を廃止して btree の search メソッドは SearhMode を取るようにする
