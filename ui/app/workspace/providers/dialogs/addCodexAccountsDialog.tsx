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
	useCodexBrowserLoginStartMutation,
	useCreateProviderKeyMutation,
} from "@/lib/store/apis/providersApi";
import { CheckCircle2, Globe2, KeyIcon, Loader2, RotateCcw, XCircle } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { v4 as uuid } from "uuid";

type Step = "choose" | "browser" | "manual";

type TokenEntry = {
	name: string;
	accessToken: string;
	refreshToken?: string;
};

type ImportResult =
	| { status: "pending" }
	| { status: "done" }
	| { status: "error"; error: string };

type BrowserResult =
	| { status: "idle" }
	| { status: "running"; message: string }
	| { status: "done"; name: string }
	| { status: "error"; error: string };

interface Props {
	open: boolean;
	onClose: () => void;
}

function isLikelyToken(value: string) {
	return value.startsWith("eyJ") || value.startsWith("sk-") || value.startsWith("sess-") || value.length > 80;
}

function pickString(obj: Record<string, unknown>, keys: string[]) {
	for (const key of keys) {
		const value = obj[key];
		if (typeof value === "string" && value.trim()) return value.trim();
	}
	return "";
}

function parseCodexAuthJSON(raw: string): TokenEntry[] {
	try {
		const parsed = JSON.parse(raw) as Record<string, unknown>;
		const tokens = typeof parsed.tokens === "object" && parsed.tokens !== null
			? parsed.tokens as Record<string, unknown>
			: parsed;
		const accessToken = pickString(tokens, ["access_token", "accessToken", "OPENAI_API_KEY", "api_key"]);
		if (!accessToken) return [];
		const refreshToken = pickString(tokens, ["refresh_token", "refreshToken"]);
		const name = pickString(tokens, ["email", "account_email", "account_id", "sub"]) || "codex-account-1";
		return [{ name, accessToken, refreshToken }];
	} catch {
		return [];
	}
}

function parseCodexTokenLines(text: string): TokenEntry[] {
	const trimmed = text.trim();
	if (!trimmed) return [];

	const jsonEntries = parseCodexAuthJSON(trimmed);
	if (jsonEntries.length > 0) return jsonEntries;

	return trimmed
		.split("\n")
		.map((line) => line.trim())
		.filter(Boolean)
		.flatMap((line, index) => {
			const parts = line.split(":").map((part) => part.trim()).filter(Boolean);
			if (parts.length >= 3 && isLikelyToken(parts[1])) {
				return [{
					name: parts[0] || `codex-account-${index + 1}`,
					accessToken: parts[1],
					refreshToken: parts.slice(2).join(":"),
				}];
			}
			if (parts.length >= 2 && isLikelyToken(parts[1])) {
				return [{
					name: parts[0] || `codex-account-${index + 1}`,
					accessToken: parts[1],
				}];
			}
			if (isLikelyToken(line)) {
				return [{ name: `codex-account-${index + 1}`, accessToken: line }];
			}
			return [];
		});
}

export default function AddCodexAccountsDialog({ open, onClose }: Props) {
	const [step, setStep] = useState<Step>("choose");
	const [manualText, setManualText] = useState("");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [results, setResults] = useState<ImportResult[]>([]);
	const [browserResult, setBrowserResult] = useState<BrowserResult>({ status: "idle" });
	const evtSourceRef = useRef<EventSource | null>(null);
	const abortRef = useRef(false);
	const [startBrowserLogin] = useCodexBrowserLoginStartMutation();
	const [createProviderKey] = useCreateProviderKeyMutation();

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
			setIsSubmitting(false);
			setResults([]);
			setBrowserResult({ status: "idle" });
		}
		return () => stopEventSource();
	}, [open, stopEventSource]);

	const entries = useMemo(() => parseCodexTokenLines(manualText), [manualText]);

	async function createKey(entry: TokenEntry) {
		const tokenValue = entry.refreshToken
			? JSON.stringify({
				access_token: entry.accessToken,
				refresh_token: entry.refreshToken,
			})
			: entry.accessToken;
		await createProviderKey({
			provider: "codex",
			key: {
				id: uuid(),
				name: entry.name,
				value: { value: tokenValue, from_env: false, env_var: "" },
				models: ["*"],
				blacklisted_models: [],
				weight: 1.0,
				enabled: true,
			} as any,
		}).unwrap();
	}

	async function handleImport() {
		if (entries.length === 0) return;
		setIsSubmitting(true);
		setResults(entries.map(() => ({ status: "pending" as const })));

		let ok = 0;
		for (let i = 0; i < entries.length; i++) {
			try {
				await createKey(entries[i]);
				ok++;
				setResults((prev) => {
					const next = [...prev];
					next[i] = { status: "done" };
					return next;
				});
			} catch (err) {
				setResults((prev) => {
					const next = [...prev];
					next[i] = { status: "error", error: getErrorMessage(err) };
					return next;
				});
			}
		}

		setIsSubmitting(false);
		if (ok > 0) {
			toast.success(`${ok} Codex account${ok > 1 ? "s" : ""} added`);
			onClose();
		}
	}

	function entryNameFromToken(accessToken: string) {
		const parts = accessToken.split(".");
		if (parts.length < 2) return "codex-account";
		try {
			const payload = JSON.parse(window.atob(parts[1].replace(/-/g, "+").replace(/_/g, "/")));
			return payload.email || payload.sub || "codex-account";
		} catch {
			return "codex-account";
		}
	}

	async function handleBrowserLogin() {
		abortRef.current = false;
		stopEventSource();
		setStep("browser");
		setIsSubmitting(true);
		setBrowserResult({ status: "running", message: "Waiting for login..." });

		let sessionId = "";
		try {
			const res = await startBrowserLogin().unwrap();
			sessionId = res.session_id;
			window.open(res.auth_url, "_blank", "noopener,noreferrer");
		} catch (err) {
			setIsSubmitting(false);
			setBrowserResult({ status: "error", error: getErrorMessage(err) });
			return;
		}

		const es = new EventSource(`/api/providers/codex/browser-login/${sessionId}`);
		evtSourceRef.current = es;

		es.addEventListener("status", async (e) => {
			if (abortRef.current) {
				stopEventSource();
				return;
			}
			try {
				const evt = JSON.parse((e as MessageEvent).data);
				if (evt.status === "done") {
					stopEventSource();
					const accessToken = evt.access_token || "";
					const refreshToken = evt.refresh_token || "";
					if (!accessToken) {
						setIsSubmitting(false);
						setBrowserResult({ status: "error", error: "Login completed without access token" });
						return;
					}
					const name = entryNameFromToken(accessToken);
					await createKey({ name, accessToken, refreshToken });
					setIsSubmitting(false);
					setBrowserResult({ status: "done", name });
					toast.success("Codex account added");
					onClose();
					return;
				}
				if (evt.status === "error") {
					stopEventSource();
					setIsSubmitting(false);
					setBrowserResult({ status: "error", error: evt.error || "Login failed" });
					return;
				}
				setBrowserResult({ status: "running", message: evt.message || "Waiting for login..." });
			} catch (err) {
				stopEventSource();
				setIsSubmitting(false);
				setBrowserResult({ status: "error", error: getErrorMessage(err) });
			}
		});

		es.onerror = () => {
			stopEventSource();
			setIsSubmitting(false);
			setBrowserResult({ status: "error", error: "Connection to server lost" });
		};
	}

	return (
		<Dialog
			open={open}
			onOpenChange={(nextOpen) => {
				if (!nextOpen && !isSubmitting) onClose();
			}}
		>
			<DialogContent className="sm:max-w-2xl" onInteractOutside={(e) => e.preventDefault()}>
				<DialogHeader>
					<DialogTitle className="flex items-center gap-3">
						<div className="flex h-10 w-10 items-center justify-center rounded-md bg-black text-white">
							<KeyIcon className="h-5 w-5" />
						</div>
						<div>
							<div>
								{step === "choose" && "Add Codex Account"}
								{step === "browser" && "Browser Login"}
								{step === "manual" && "Import Codex Tokens"}
							</div>
							{step === "choose" && (
								<p className="text-muted-foreground mt-1 text-sm font-normal">Choose how to add</p>
							)}
						</div>
					</DialogTitle>
				</DialogHeader>

				{step === "choose" && (
					<div className="grid grid-cols-2 gap-5 py-4">
						<button
							type="button"
							onClick={handleBrowserLogin}
							className="border-border hover:border-primary hover:bg-accent flex min-h-40 flex-col items-center justify-center gap-4 rounded-lg border-2 p-6 transition-colors"
						>
							<Globe2 className="h-10 w-10 text-cyan-400" />
							<div className="text-center">
								<div className="text-lg font-semibold">Browser Login</div>
								<div className="text-muted-foreground mt-2 text-sm">Login via ChatGPT</div>
							</div>
						</button>
						<button
							type="button"
							onClick={() => setStep("manual")}
							className="border-primary/60 bg-primary/5 hover:bg-primary/10 flex min-h-40 flex-col items-center justify-center gap-4 rounded-lg border-2 p-6 transition-colors"
						>
							<KeyIcon className="h-10 w-10 text-yellow-400" />
							<div className="text-center">
								<div className="text-lg font-semibold">Import Tokens</div>
								<div className="text-muted-foreground mt-2 text-sm">Paste access & refresh token</div>
							</div>
						</button>
					</div>
				)}

				{step === "browser" && (
					<div className="flex min-h-64 flex-col items-center justify-center gap-6 py-8 text-center">
						<div className="flex h-16 w-16 items-center justify-center rounded-full bg-green-500/10 text-green-500">
							{browserResult.status === "error" ? (
								<XCircle className="h-8 w-8" />
							) : browserResult.status === "done" ? (
								<CheckCircle2 className="h-8 w-8" />
							) : (
								<RotateCcw className="h-8 w-8 animate-spin" />
							)}
						</div>
						<div>
							<div className="text-lg font-semibold">
								{browserResult.status === "error" && "Login failed"}
								{browserResult.status === "done" && "Login complete"}
								{browserResult.status !== "error" && browserResult.status !== "done" && "Waiting for login..."}
							</div>
							<div className="text-muted-foreground mt-5 text-base">
								{browserResult.status === "error" && browserResult.error}
								{browserResult.status === "done" && `${browserResult.name} added`}
								{browserResult.status !== "error" && browserResult.status !== "done" && "Complete the login in the browser window."}
							</div>
						</div>
					</div>
				)}

				{step === "manual" && (
					<div className="py-2">
						<TokenImportForm
							manualText={manualText}
							setManualText={setManualText}
							entries={entries}
							results={results}
						/>
					</div>
				)}

				<DialogFooter>
					{step === "choose" ? (
						<Button variant="outline" onClick={onClose}>
							Cancel
						</Button>
					) : step === "browser" ? (
						<div className="flex w-full justify-center gap-2">
							<Button variant="outline" onClick={onClose} disabled={isSubmitting}>
								Cancel
							</Button>
							{browserResult.status === "error" && (
								<Button onClick={handleBrowserLogin}>
									Retry
								</Button>
							)}
						</div>
					) : (
						<div className="flex w-full justify-between gap-2">
							<Button variant="outline" onClick={() => setStep("choose")} disabled={isSubmitting}>
								Back
							</Button>
							<Button onClick={handleImport} disabled={entries.length === 0 || isSubmitting} isLoading={isSubmitting}>
								Add {entries.length > 0 ? `(${entries.length})` : ""}
							</Button>
						</div>
					)}
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}

function TokenImportForm({
	manualText,
	setManualText,
	entries,
	results,
}: {
	manualText: string;
	setManualText: (value: string) => void;
	entries: TokenEntry[];
	results: ImportResult[];
}) {
	return (
		<div className="flex flex-col gap-3">
			<label className="text-sm font-medium">
				Tokens <span className="text-muted-foreground font-normal">(Codex auth JSON, access token, or name:access:refresh)</span>
			</label>
			<Textarea
				placeholder={'{"tokens":{"access_token":"eyJ...","refresh_token":"...","account_id":"..."}}\n\nor\nuser@example.com:eyJ...:refresh_token_here'}
				rows={7}
				value={manualText}
				onChange={(e) => setManualText(e.target.value)}
				className="font-mono text-xs"
				autoFocus
			/>
			<div className="text-muted-foreground text-xs">{entries.length} account{entries.length === 1 ? "" : "s"} detected</div>
			{entries.length > 0 && (
				<div className="flex max-h-48 flex-col gap-1.5 overflow-y-auto">
					{entries.map((entry, i) => {
						const result = results[i];
						return (
							<div key={`${entry.name}-${i}`} className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm">
								{!result && <div className="h-4 w-4 rounded-full border-2 border-muted shrink-0" />}
								{result?.status === "pending" && <Loader2 className="h-4 w-4 animate-spin text-primary shrink-0" />}
								{result?.status === "done" && <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />}
								{result?.status === "error" && <XCircle className="h-4 w-4 text-destructive shrink-0" />}
								<span className="font-medium">{entry.name}</span>
								<span className="text-muted-foreground truncate font-mono text-xs">{entry.accessToken.slice(0, 14)}...</span>
								{entry.refreshToken && <span className="text-muted-foreground text-xs">refresh parsed</span>}
								{result?.status === "error" && <span className="text-destructive truncate text-xs">{result.error}</span>}
							</div>
						);
					})}
				</div>
			)}
		</div>
	);
}
