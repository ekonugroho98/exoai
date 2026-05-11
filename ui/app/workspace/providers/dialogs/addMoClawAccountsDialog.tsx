"use client";

import { Button } from "@/components/ui/button";
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
	useMoclawLoginMutation,
} from "@/lib/store/apis/providersApi";
import { CheckCircle2, KeyIcon, Loader2, MonitorSmartphone, XCircle } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import { v4 as uuid } from "uuid";

type Step = "choose" | "browser" | "manual";

interface BrowserLoginStatus {
	status: "launching" | "connecting" | "navigating" | "waiting" | "done" | "error";
	message?: string;
	access_token?: string;
	refresh_token?: string;
	expires_in?: number;
	error?: string;
}

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
	const [browserStatus, setBrowserStatus] = useState<BrowserLoginStatus | null>(null);
	const [isBrowserRunning, setIsBrowserRunning] = useState(false);
	const evtSourceRef = useRef<EventSource | null>(null);

	// ── API hooks ─────────────────────────────────────────────────────────────
	const [startBrowserLogin] = useMoclawBrowserLoginStartMutation();
	const [createProviderKey] = useCreateProviderKeyMutation();

	// ── Cleanup on unmount or dialog close ───────────────────────────────────
	const stopEventSource = useCallback(() => {
		if (evtSourceRef.current) {
			evtSourceRef.current.close();
			evtSourceRef.current = null;
		}
	}, []);

	useEffect(() => {
		if (!open) {
			stopEventSource();
			setStep("choose");
			setManualText("");
			setBrowserStatus(null);
			setIsBrowserRunning(false);
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

	// ── Browser login ─────────────────────────────────────────────────────────
	async function handleBrowserLogin() {
		setIsBrowserRunning(true);
		setBrowserStatus({ status: "launching", message: "Starting browser..." });

		let sessionId: string;
		try {
			const res = await startBrowserLogin().unwrap();
			sessionId = res.session_id;
		} catch (err) {
			setBrowserStatus({ status: "error", error: getErrorMessage(err) });
			setIsBrowserRunning(false);
			return;
		}

		// Connect to SSE stream
		const es = new EventSource(`/api/providers/moclaw/browser-login/${sessionId}`);
		evtSourceRef.current = es;

		es.addEventListener("status", async (e) => {
			try {
				const evt: BrowserLoginStatus = JSON.parse((e as MessageEvent).data);
				setBrowserStatus(evt);

				if (evt.status === "done") {
					es.close();
					evtSourceRef.current = null;
					const token = evt.refresh_token || evt.access_token || "";
					if (token) {
						try {
							await createKey(`moclaw-${Date.now()}`, token);
							toast.success("Account added successfully");
							setIsBrowserRunning(false);
							setBrowserStatus(null);
							// Stay on browser step so user can add more accounts
						} catch (err) {
							setBrowserStatus({ status: "error", error: `Key save failed: ${getErrorMessage(err)}` });
						}
					}
					setIsBrowserRunning(false);
				} else if (evt.status === "error") {
					es.close();
					evtSourceRef.current = null;
					setIsBrowserRunning(false);
				}
			} catch {}
		});

		es.onerror = () => {
			es.close();
			evtSourceRef.current = null;
			if (isBrowserRunning) {
				setBrowserStatus({ status: "error", error: "Connection to server lost" });
				setIsBrowserRunning(false);
			}
		};
	}

	// ── Manual login ──────────────────────────────────────────────────────────
	// Parse lines of format "email:token" or just "token"
	function parseManualLines(): Array<{ name: string; token: string }> {
		return manualText
			.split("\n")
			.map((l) => l.trim())
			.filter(Boolean)
			.map((line, i) => {
				// Detect "email:token" — token part starts with eyJ (JWT) or v1.
				const colonIdx = line.indexOf(":");
				if (colonIdx > 0) {
					const possibleEmail = line.slice(0, colonIdx).trim();
					const possibleToken = line.slice(colonIdx + 1).trim();
					// Only treat as email:token if token looks like a real token
					if (possibleToken.startsWith("eyJ") || possibleToken.startsWith("v1.")) {
						return { name: possibleEmail || `moclaw-account-${i + 1}`, token: possibleToken };
					}
				}
				// Plain token — auto name
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

	// ── Status icon helper ────────────────────────────────────────────────────
	function StatusIcon({ status }: { status: BrowserLoginStatus["status"] }) {
		if (status === "done") return <CheckCircle2 className="h-5 w-5 text-green-500" />;
		if (status === "error") return <XCircle className="h-5 w-5 text-destructive" />;
		return <Loader2 className="h-5 w-5 animate-spin text-primary" />;
	}

	const manualEntries = parseManualLines();
	const manualLineCount = manualEntries.length;

	return (
		<Dialog
			open={open}
			onOpenChange={(o) => {
				if (!o && !isBrowserRunning) onClose();
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
						{!isBrowserRunning && !browserStatus && (
							<div className="text-muted-foreground rounded-lg border p-4 text-sm">
								<p className="font-medium text-foreground mb-1">How it works</p>
								<ol className="list-decimal list-inside space-y-1">
									<li>Click <strong>Start Browser</strong> below</li>
									<li>A Chrome window opens at moclaw.ai</li>
									<li>Click <strong>Get Started</strong> → <strong>Continue with Google</strong></li>
									<li>Complete Google login</li>
									<li>Tokens are captured and saved automatically</li>
								</ol>
								<p className="mt-2 text-xs">Repeat for each additional account.</p>
							</div>
						)}

						{browserStatus && (
							<div className="rounded-lg border p-4 space-y-3">
								<div className="flex items-center gap-2">
									<StatusIcon status={browserStatus.status} />
									<span className="font-medium capitalize">
										{browserStatus.status === "done" ? "Login successful!" : browserStatus.status}
									</span>
								</div>
								{(browserStatus.message || browserStatus.error) && (
									<p className={`text-sm ${browserStatus.status === "error" ? "text-destructive" : "text-muted-foreground"}`}>
										{browserStatus.error || browserStatus.message}
									</p>
								)}
								{browserStatus.status === "done" && (
									<Button
										size="sm"
										onClick={() => {
											setBrowserStatus(null);
										}}
									>
										Add another account
									</Button>
								)}
							</div>
						)}

						{!isBrowserRunning && (
							<Button onClick={handleBrowserLogin} className="w-full">
								<MonitorSmartphone className="mr-2 h-4 w-4" />
								{browserStatus?.status === "done" ? "Start new browser login" : "Start Browser"}
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
						<Button
							variant="outline"
							onClick={() => {
								stopEventSource();
								setIsBrowserRunning(false);
								setBrowserStatus(null);
								setStep("choose");
							}}
							disabled={isBrowserRunning}
						>
							Back
						</Button>
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
