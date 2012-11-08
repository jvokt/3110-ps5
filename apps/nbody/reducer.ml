let (body, accs) = Program.get_input() in
let accs = List.map Util.unmarshal accs in
let a = List.fold_left Plane.v_plus (0.0, 0.0) accs in
let half_a = Plane.scale_point 0.5 a in
let b = Util.unmarshal body in
let (m,p0,v0) = b in
let body' = (m, Plane.v_plus p0 (Plane.v_plus v0 half_a), Plane.v_plus v0 a)
in Program.set_output [Util.marshal body']
