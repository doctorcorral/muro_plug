[
  muro: "0.22.38",
  fuel: 5_000_000,
  modules: [
    {"priv/muro/http/step.muro", MuroPlug.Step, "lib/muro_plug/step.ex"},
    {"priv/muro/http/step.muro", MuroPlug.StepBookTest, "test/step_book_test.exs",
     [kind: :exunit, impl: MuroPlug.Step, imports: true]}
  ]
]
