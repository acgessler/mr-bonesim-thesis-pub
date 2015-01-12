##################################################################
# Logic omitted for copyright reasons.
# Email me.
##################################################################

# Test version of the original simulation script.
#
# Reads tuples from stdin and writes an output 2-tuple to
# stdout (both linewise)
#
# Solves a system of ODEs of the same complexity, but
# with dummy parameter values. To avoid cache or memoization,
# the system depends on actual input values.

# Keep in sync with the copy that is directly embedded
# into WorkerNode.jar for use with the baseline prototype.

function dxdt = f (x,t)
        # omitted for copyright reasons
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

