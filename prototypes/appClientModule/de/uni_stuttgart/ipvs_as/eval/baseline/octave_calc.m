# Test version of the original simulation script.
#
# Reads tuples from stdin and writes an output 2-tuple to
# stdout (both linewise)
#
# Solves a system of ODEs of the same complexity, but
# with dummy parameter values. To avoid cache or memoization,
# the system depends on actual input values.

# Keep in sync with OCTAVE_SCRIPT in the WorkerNode.java
# of the baseline prototype if use of the baseline
# prototype is intended.

function dxdt = f (x,t)
        Cs   = 1.0;
        D_A  = 1.0;
        d_B  = 1.0;
        D_C  = 1.0;
        D_R  = 1.0;
        f_0  = 1.0;
        K    = 1.0;
        k1   = 1.0;
        k2   = 1.0;
        k3   = 1.0;
        k4   = 1.0;
        k5   = 1.0;
        k6   = 1.0;
        k_B  = 1.0;
        K_lp = 1.0;
        k_O  = 1.0;
        K_op = 1.0;
        k_p  = 1.0;
        r_L  = 1.0;
        S_p  = 1.0;

        F_s = 1.0;
        K_s = 1.0;
        K_m = 1.0;

        k_Yd = 1.0;
        k_cY = 1.0;
        f_s = 1.0;
        k_ypg = 1.0;
        k_pgd = 1.0;
        k_fs = 1.0;
        k_y = 1.0;
        k_Rp = 1.0;

        Pi_B = 1.0;
        m_max = 1.0;
        Ve = 1.0;

        k_mC = 1.0;
        k_sB = 1.0;

        R = x(1);
        B = x(2);
        Y = x(3);
        C = x(4);
        s = x(5);
        m = x(6);

        D_B = f_0*d_B;
        Pi_C = (C+f_0*Cs)/(C+Cs);
        Pi_P = S_p/k_p/(k6/k5);
        Pi_L = k3/k4 * K_lp*Pi_P*B/(1+k3*K/k4+k1/(k2*k_O)*(K_op*R/Pi_P));
        p = k_ypg/k_pgd * f_s*Y / (1 + exp(-k_fs*f_s -k_y*Y));

        dRdt = D_R*Pi_C - D_B/Pi_C * R + k_Rp*p/(1+p);
        dBdt = D_B/Pi_C * R - k_B * B;
        dYdt = Pi_B*k_B*B - k_Yd*Y + k_cY;
        dCdt = D_C*Pi_L - D_A*Pi_C*C;
        dsdt = 0;
        dmdt = k_sB*B*s/(K_s + s) * (1-m/m_max) - k_mC*C*m/(K_m + m);

        dxdt=zeros(6,1);
        dxdt(1,1) = dRdt;
        dxdt(2,1) = dBdt;
        dxdt(3,1) = dYdt;
        dxdt(4,1) = dCdt;
        dxdt(5,1) = dsdt;
        dxdt(6,1) = dmdt;
endfunction

try
        while 1
                vals = sscanf(fgetl(stdin), '%f');
                if (length(vals) == 0) continue; endif

                # Solve a system of ODE's. Make sure to add a
                # dependency on input.
                sol = lsode("f", [vals(1);2;vals(end);4;5;vals(end)], (t = linspace(0, 10, 3)));

                # We do not care about the exact result,
                # so calculate two values and make sure to
		# add a dependency on the solution of the ODE.
                res = [sol(end, 6), sol(end, 5) * sol(end, 1)];
                printf('%f ', res);
                printf('\n');
                fflush(stdout);
        end

# Control reaches here upon CTRL-D (SIGQUIT),
# or if the pipes break when the coupled reducer terminates.
#
# Note that this also catches every other error, so take care.
catch
        printf(lasterr);
end

