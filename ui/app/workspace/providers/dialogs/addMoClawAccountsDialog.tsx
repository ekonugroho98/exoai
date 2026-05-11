"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { getErrorMessage } from "@/lib/store";
import {
	useCreateProviderKeyMutation,
	useMoclawBrowserLoginStartMutation,
} from "@/lib/store/apis/providersApi";
import { CheckCircle2, KeyIcon, Loader2, MonitorSmartphone, XCircle } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import { v4 as uuid } from "uuid";

type Step = "choose" | "browser" | "manual";

type AccountResult =
	| { status: "pending" }
	| { status: "running"; message: string }
	| { status: "done" }
	| { status: "error"; error: string };

interface Props {
	open: boolean;
	onClose: () => void;
}

export default function AddMoClawAccountsDialog({ open, onClose }: Props) {
	const [step, setStep] = useState<Step>("choose");

	// ── Manual state ──────────────────────────────────────────────────────────
	const [manualText, setManualText] = useState("");
	const [isManualSubmitting, setIsManualSubmitting] = useState(false);

	// ── Browser-login state ───────────────────────────────────────────────────
	const [accountCount, setAccountCount] = useState(1);
	const [isRunning, setIsRunning] = useState(false);
	const [results, setResults] = useState<AccountResult[]>([]);
	const evtSourceRef = useRef<EventSource | null>(null);
	const abortRef = useRef(false);

	// ── API hooks ─────────────────────────────────────────────────────────────
	const [startBrowserLogin] = useMoclawBrowserLoginStartMutation();
	const [createProviderKey] = useCreateProviderKeyMutation();

	// ── Cleanup ───────────────────────────────────────────────────────────────
	const stopEventSource = useCallback(() => {
		evtSourceRef.current?.close();
		evtSourceRef.current = null;
	}, []);

	useEffect(() => {
		if (!open) {
			abortRef.current = true;
			stopEventSource();
			setStep("choose");
			setManualText("");
			setResults([]);
			setIsRunning(false);
			setAccountCount(1);
		}
		return () => stopEventSource();
	}, [open, stopEventSource]);

	// ── Helpers ───────────────────────────────────────────────────────────────
	async function createKey(name: string, tokenValue: string) {
		await createProviderKey({
			provider: "moclaw",
			key: {
				id: uuid(),
				name,
				value: { value: tokenValue, from_env: false, env_var: "" },
				models: ["*"],
				blacklisted_models: [],
				weight: 1.0,
				enabled: true,
			} as any,
		}).unwrap();
	}

	function setResult(index: number, result: AccountResult) {
		setResults((prev) => {
			const next = [...prev];
			next[index] = result;
			return next;
		});
	}

	// Run a single browser login session for one account and return success/fail
	async function runOneSession(index: number): Promise<boolean> {
		setResult(index, { status: "running", message: "Starting browser..." });

		let sessionId: string;
		try {
			const res = await startBrowserLogin().unwrap();
			sessionId = res.session_id;
		} catch (err) {
			setResult(index, { status: "error", error: getErrorMessage(err) });
			return false;
		}

		return new Promise<boolean>((resolve) => {
			const es = new EventSource(`/api/providers/moclaw/browser-login/${sessionId}`);
			evtSourceRef.current = es;

			es.addEventListener("status", async (e) => {
				if (abortRef.current) {
					es.close();
					resolve(false);
					return;
				}
				try {
					const evt = JSON.parse((e as MessageEvent).data);
					if (evt.status === "done") {
						es.close();
						evtSourceRef.current = null;
						const token = evt.refresh_token || evt.access_token || "";
						try {
							await createKey(`moclaw-account-${index + 1}`, token);
							setResult(index, { status: "done" });
							resolve(true);
						} catch (err) {
							setResult(index, { status: "error", error: `Key save failed: ${getErrorMessage(err)}` });
							resolve(false);
						}
					} else if (evt.status === "error") {
						es.close();
						evtSourceRef.current = null;
						setResult(index, { status: "error", error: evt.error || "Login failed" });
						resolve(false);
					} else {
						// Progress update
						setResult(index, { status: "running", message: evt.message || evt.status });
					}
				} catch {
					resolve(false);
				}
			});

			es.onerror = () => {
				es.close();
				evtSourceRef.current = null;
				setResult(index, { status: "error", error: "Connection to server lost" });
				resolve(false);
			};
		});
	}

	// ── Start all sessions sequentially ───────────────────────────────────────
	async function handleBrowserLoginAll() {
		abortRef.current = false;
		setIsRunning(true);
		// Initialise all slots as pending
		setResults(Array.from({ length: accountCount }, () => ({ status: "pending" as const })));

		let successCount = 0;
		for (let i = 0; i < accountCount; i++) {
			if (abortRef.current) break;
			const ok = await runOneSession(i);
			if (ok) successCount++;
		}

		setIsRunning(false);
		if (successCount > 0) {
			toast.success(`${successCount} of ${accountCount} account${accountCount > 1 ? "s" : ""} added`);
		}
	}

	// ── Manual login ──────────────────────────────────────────────────────────
	function parseManualLines(): Array<{ name: string; token: string }> {
		return manualText
			.split("\n")
			.map((l) => l.trim())
			.filter(Boolean)
			.map((line, i) => {
				const colonIdx = line.indexOf(":");
				if (colonIdx > 0) {
					const possibleEmail = line.slice(0, colonIdx).trim();
					const possibleToken = line.slice(colonIdx + 1).trim();
					if (possibleToken.startsWith("eyJ") || possibleToken.startsWith("v1.")) {
						return { name: possibleEmail || `moclaw-account-${i + 1}`, token: possibleToken };
					}
				}
				return { name: `moclaw-account-${i + 1}`, token: line };
			});
	}

	async function handleManual() {
		const entries = parseManualLines();
		if (!entries.length) return;
		setIsManualSubmitting(true);
		let ok = 0;
		for (const entry of entries) {
			try {
				await createKey(entry.name, entry.token);
				ok++;
			} catch (err) {
				toast.error(`"${entry.name}" failed`, { description: getErrorMessage(err) });
			}
		}
		setIsManualSubmitting(false);
		if (ok > 0) {
			toast.success(`${ok} account${ok > 1 ? "s" : ""} added`);
			onClose();
		}
	}

	const manualEntries = parseManualLines();
	const manualLineCount = manualEntries.length;
	const allDone = results.length > 0 && results.every((r) => r.status === "done" || r.status === "error");
	const anySuccess = results.some((r) => r.status === "done");

	return (
		<Dialog
			open={open}
			onOpenChange={(o) => {
				if (!o && !isRunning) onClose();
			}}
		>
			<DialogContent className="sm:max-w-md" onInteractOutside={(e) => e.preventDefault()}>
				<DialogHeader>
					<DialogTitle className="flex items-center gap-2">
						<img
							src="/providers/moclaw.svg"
							alt=""
							className="h-6 w-6"
							onError={(e) => (e.currentTarget.style.display = "none")}
						/>
						{step === "choose" && "Add MoClaw Accounts"}
						{step === "browser" && "Auto Login via Google"}
						{step === "manual" && "Manual — Paste Tokens"}
					</DialogTitle>
					{step === "choose" && (
						<p className="text-muted-foreground mt-1 text-sm">Choose how to add accounts</p>
					)}
				</DialogHeader>

				{/* ── Step 1: choose ─────────────────────────────────────────── */}
				{step === "choose" && (
					<div className="grid grid-cols-2 gap-3 py-2">
						<button
							onClick={() => setStep("browser")}
							className="border-border hover:border-primary hover:bg-accent flex flex-col items-center gap-3 rounded-lg border-2 p-6 transition-colors text-left"
						>
							<MonitorSmartphone className="text-primary h-8 w-8" />
							<div className="text-center">
								<div className="font-semibold">Auto Login</div>
								<div className="text-muted-foreground text-xs mt-1">Opens browser — login with Google</div>
							</div>
						</button>
						<button
							onClick={() => setStep("manual")}
							className="border-border hover:border-primary hover:bg-accent flex flex-col items-center gap-3 rounded-lg border-2 p-6 transition-colors text-left"
						>
							<KeyIcon className="text-primary h-8 w-8" />
							<div className="text-center">
								<div className="font-semibold">Manual</div>
								<div className="text-muted-foreground text-xs mt-1">Paste refresh / access token</div>
							</div>
						</button>
					</div>
				)}

				{/* ── Step 2a: browser login ──────────────────────────────────── */}
				{step === "browser" && (
					<div className="flex flex-col gap-4 py-2">
						{/* Account count picker — only shown before starting */}
						{!isRunning && results.length === 0 && (
							<>
								<div className="flex items-center gap-3">
									<label className="text-sm font-medium whitespace-nowrap">Number of accounts</label>
									<Input
										type="number"
										min={1}
										max={20}
										value={accountCount}
										onChange={(e) => setAccountCount(Math.max(1, Math.min(20, Number(e.target.value) || 1)))}
										className="w-24 font-mono text-center"
									/>
								</div>
								<div className="text-muted-foreground rounded-lg border p-4 text-sm">
									<p className="font-medium text-foreground mb-1">How it works</p>
									<ol className="list-decimal list-inside space-y-1">
										<li>Click <strong>Start</strong> — a Chrome window opens at moclaw.ai</li>
										<li>Click <strong>Get Started</strong> → <strong>Continue with Google</strong></li>
										<li>Complete Google login in the browser window</li>
										<li>Tokens are captured and saved automatically</li>
										{accountCount > 1 && <li>Next window opens automatically for the next account</li>}
									</ol>
								</div>
							</>
						)}

						{/* Progress list */}
						{results.length > 0 && (
							<div className="flex flex-col gap-1.5 max-h-64 overflow-y-auto">
								{results.map((r, i) => (
									<div key={i} className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm">
										{r.status === "pending" && <div className="h-4 w-4 rounded-full border-2 border-muted" />}
										{r.status === "running" && <Loader2 className="h-4 w-4 animate-spin text-primary shrink-0" />}
										{r.status === "done" && <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />}
										{r.status === "error" && <XCircle className="h-4 w-4 text-destructive shrink-0" />}
										<span className="font-medium">Account {i + 1}</span>
										{r.status === "running" && (
											<span className="text-muted-foreground text-xs truncate">{r.message}</span>
										)}
										{r.status === "error" && (
											<span className="text-destructive text-xs truncate">{r.error}</span>
										)}
										{r.status === "done" && (
											<span className="text-green-600 text-xs">Saved ✓</span>
										)}
										{r.status === "pending" && (
											<span className="text-muted-foreground text-xs">Waiting...</span>
										)}
									</div>
								))}
							</div>
						)}

						{/* Action buttons */}
						{!isRunning && results.length === 0 && (
							<Button onClick={handleBrowserLoginAll} className="w-full">
								<MonitorSmartphone className="mr-2 h-4 w-4" />
								Start ({accountCount} account{accountCount > 1 ? "s" : ""})
							</Button>
						)}

						{!isRunning && allDone && (
							<Button
								variant="outline"
								onClick={() => {
									setResults([]);
									setAccountCount(1);
								}}
								className="w-full"
							>
								Add more accounts
							</Button>
						)}
					</div>
				)}

				{/* ── Step 2b: manual tokens ──────────────────────────────────── */}
				{step === "manual" && (
					<div className="flex flex-col gap-3 py-2">
						<label className="text-sm font-medium">
							Refresh Tokens{" "}
							<span className="text-muted-foreground font-normal">(one per line — email:token optional)</span>
						</label>
						<Textarea
							placeholder={"v1.MTI4NjQ...                  ← refresh_token (preferred)\nuser@gmail.com:v1.NjI4MA...   ← with email label"}
							rows={6}
							value={manualText}
							onChange={(e) => setManualText(e.target.value)}
							className="font-mono text-xs"
							autoFocus
						/>
						{manualLineCount > 0 && (
							<div className="text-muted-foreground text-xs space-y-0.5">
								{manualEntries.map((e, i) => (
									<div key={i} className="flex gap-2">
										<span className="font-mono">{i + 1}.</span>
										<span className="font-medium text-foreground/80">{e.name}</span>
										<span className="font-mono opacity-50">{e.token.slice(0, 12)}...</span>
									</div>
								))}
							</div>
						)}
						{manualLineCount === 0 && <p className="text-muted-foreground text-xs">0 keys</p>}
					</div>
				)}

				<DialogFooter className="gap-2 sm:gap-0">
					{step === "choose" ? (
						<Button variant="outline" onClick={onClose}>
							Cancel
						</Button>
					) : step === "browser" ? (
						<div className="flex w-full justify-between gap-2">
							<Button
								variant="outline"
								onClick={() => {
									abortRef.current = true;
									stopEventSource();
									setIsRunning(false);
									setResults([]);
									setStep("choose");
								}}
								disabled={isRunning}
							>
								Back
							</Button>
							{allDone && anySuccess && (
								<Button onClick={onClose}>Done</Button>
							)}
							{isRunning && (
								<Button
									variant="destructive"
									onClick={() => {
										abortRef.current = true;
										stopEventSource();
										setIsRunning(false);
									}}
								>
									Stop
								</Button>
							)}
						</div>
					) : (
						<>
							<Button variant="outline" onClick={() => setStep("choose")} disabled={isManualSubmitting}>
								Back
							</Button>
							<Button
								onClick={handleManual}
								disabled={manualLineCount === 0 || isManualSubmitting}
								isLoading={isManualSubmitting}
							>
								Add {manualLineCount > 0 ? `(${manualLineCount})` : ""}
							</Button>
						</>
					)}
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}
