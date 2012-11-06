let (file, contents) = Program.get_input() in
Program.set_output (List.fold_left (fun acc word -> (word, file)::acc) []
                   (List.map String.lowercase (Util.split_words contents)))
