let (word, docs) = Program.get_input() in
  Program.set_output (List.sort compare docs)
