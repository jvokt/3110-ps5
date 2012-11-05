let (id, accs) = Program.get_input() in
let accs = List.map Util.unmarshal accs in
let marshaled = Util.marshal (List.fold_left Plane.v_plus (0.0, 0.0) accs) in
Program.set_output [marshaled]

(*
let (id, accelerations) = Program.get_input() in
let accs = Util.unmarshal accelerations
let sum net acc = Plane.v_plus net acc) in
let net_acc = List.fold_left sum (0.0,0.0) accs in
let marshaled = Util.marshal net_acc in
Program.set_output [marshaled]
*)