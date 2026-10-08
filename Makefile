.PHONY: verify-phase0 verify-phase1 verify-phase2 verify-phase3 verify-phase4 verify-phase5 verify-phase6 verify-phase7 benchmark benchmark-smoke verify-clean-clone demo verify

demo:
	@python3 scripts/demo.py

verify-phase0:
	@./scripts/verify-phase0.sh

verify-phase1:
	@./scripts/verify-phase1.sh

verify-phase2:
	@./scripts/verify-phase2.sh

verify-phase3:
	@./scripts/verify-phase3.sh

verify-phase4:
	@./scripts/verify-phase4.sh

verify-phase5:
	@./scripts/verify-phase5.sh

verify-phase6:
	@./scripts/verify-phase6.sh

verify-phase7:
	@./scripts/verify-phase7.sh

benchmark:
	@python3 scripts/benchmark.py --output docs/benchmarks/raw.json

benchmark-smoke:
	@python3 scripts/benchmark.py --smoke

verify-clean-clone:
	@./scripts/clean-clone.sh

verify: verify-phase7
	@if [ "$${REORGGUARD_CLEAN_CLONE:-0}" != 1 ]; then $(MAKE) verify-clean-clone; fi
	@echo 'PASS full local release-candidate verification.'
