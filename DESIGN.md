PlanNode は AccessMethod を掘っているべき
つまり HaveAccessMethod を継承している必要がある
それによって PlanNode は AccessMethod の関連型の SearchOption にも関連し
結果 AccessMessod の search にその SearchOption を関連させることができるはず
