let (id, accs) = Program.get_input() in
let accs = List.map Util.unmarshal accs in
let marshaled = Util.marshal (List.fold_left Plane.v_plus (0.0, 0.0) accs) in
Program.set_output [marshaled]
