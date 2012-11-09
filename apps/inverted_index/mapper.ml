let (file, contents) = Program.get_input() in
let used = Hashtbl.create 16 in
let just_once acc word =
  if Hashtbl.mem used word then acc
  else (Hashtbl.add used word "present"; (word, file)::acc)
in
Program.set_output (List.fold_left just_once []
                   (List.map String.lowercase (Util.split_words contents)))
