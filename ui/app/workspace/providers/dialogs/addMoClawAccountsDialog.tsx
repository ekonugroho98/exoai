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
import { useCreateProviderKeyMutation, useMoclawLoginMutation } from "@/lib/store/apis/providersApi";
import { KeyIcon, MonitorSmartphone } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { v4 as uuid } from "uuid";

type Step = "choose" | "auto" | "manual";

interface Props {
	open: boolean;
	onClose: () => void;
}

export default function AddMoClawAccountsDialog({ open, onClose }: Props) {
	const [step, setStep] = useState<Step>("choose");
	const [inputText, setInputText] = useState("");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [moclawLogin] = useMoclawLoginMutation();
	const [createProviderKey] = useCreateProviderKeyMutation();

	function reset() {
		setStep("choose");
		setInputText("");
		setIsSubmitting(false);
	}

	function handleClose() {
		reset();
		onClose();
	}

	// Parse non-empty lines from the textarea
	function parseLines(): string[] {
		return inputText
			.split("\n")
			.map((l) => l.trim())
			.filter(Boolean);
	}

	const lineCount = parseLines().length;

	// Create a Bifrost key from a raw token value
	async function createKey(name: string, tokenValue: string): Promise<void> {
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

	async function handleAutoLogin() {
		const lines = parseLines();
		if (lines.length === 0) return;

		setIsSubmitting(true);
		let successCount = 0;
		const errors: string[] = [];

		for (const line of lines) {
			const sepIdx = line.indexOf(":");
			if (sepIdx === -1) {
				errors.push(`"${line}" — invalid format (expected email:password)`);
				continue;
			}
			const email = line.slice(0, sepIdx).trim();
			const password = line.slice(sepIdx + 1).trim();
			if (!email || !password) {
				errors.push(`"${line}" — email or password is empty`);
				continue;
			}
			try {
				const result = await moclawLogin({ email, password }).unwrap();
				const tokenValue = result.refresh_token || result.access_token;
				await createKey(email, tokenValue);
				successCount++;
			} catch (err) {
				errors.push(`${email}: ${getErrorMessage(err)}`);
			}
		}

		setIsSubmitting(false);

		if (successCount > 0) {
			toast.success(`${successCount} account${successCount > 1 ? "s" : ""} added successfully`);
		}
		if (errors.length > 0) {
			for (const e of errors) {
				toast.error("Failed to add account", { description: e });
			}
		}
		if (successCount > 0) {
			handleClose();
		}
	}

	async function handleManual() {
		const lines = parseLines();
		if (lines.length === 0) return;

		setIsSubmitting(true);
		let successCount = 0;
		const errors: string[] = [];

		for (let i = 0; i < lines.length; i++) {
			const token = lines[i].trim();
			const name = `moclaw-account-${i + 1}`;
			try {
				await createKey(name, token);
				successCount++;
			} catch (err) {
				errors.push(`Line ${i + 1}: ${getErrorMessage(err)}`);
			}
		}

		setIsSubmitting(false);

		if (successCount > 0) {
			toast.success(`${successCount} account${successCount > 1 ? "s" : ""} added successfully`);
		}
		if (errors.length > 0) {
			for (const e of errors) {
				toast.error("Failed to add key", { description: e });
			}
		}
		if (successCount > 0) {
			handleClose();
		}
	}

	return (
		<Dialog
			open={open}
			onOpenChange={(o) => {
				if (!o) handleClose();
			}}
		>
			<DialogContent className="sm:max-w-md" onInteractOutside={(e) => e.preventDefault()}>
				<DialogHeader>
					<DialogTitle className="flex items-center gap-2">
						<img src="/providers/moclaw.svg" alt="MoClaw" className="h-6 w-6" onError={(e) => (e.currentTarget.style.display = "none")} />
						{step === "choose" && "Add MoClaw Accounts"}
						{step === "auto" && "Auto Login — Email & Password"}
						{step === "manual" && "Manual — Paste Tokens"}
					</DialogTitle>
					{step === "choose" && (
						<p className="text-muted-foreground mt-1 text-sm">Choose how to add accounts</p>
					)}
				</DialogHeader>

				{/* Step 1: choose */}
				{step === "choose" && (
					<div className="grid grid-cols-2 gap-3 py-2">
						<button
							onClick={() => setStep("auto")}
							className="border-border hover:border-primary hover:bg-accent flex flex-col items-center gap-3 rounded-lg border-2 p-6 transition-colors"
						>
							<MonitorSmartphone className="text-primary h-8 w-8" />
							<div className="text-center">
								<div className="font-semibold">Auto Login</div>
								<div className="text-muted-foreground text-xs">Email &amp; password</div>
							</div>
						</button>
						<button
							onClick={() => setStep("manual")}
							className="border-primary bg-accent border-2 flex flex-col items-center gap-3 rounded-lg p-6 transition-colors"
						>
							<KeyIcon className="text-primary h-8 w-8" />
							<div className="text-center">
								<div className="font-semibold">Manual</div>
								<div className="text-muted-foreground text-xs">Paste API key / token</div>
							</div>
						</button>
					</div>
				)}

				{/* Step 2a: auto login */}
				{step === "auto" && (
					<div className="flex flex-col gap-3 py-2">
						<label className="text-sm font-medium">
							Accounts{" "}
							<span className="text-muted-foreground font-normal">(email:password, one per line)</span>
						</label>
						<Textarea
							placeholder={"email1@gmail.com:password123\nemail2@gmail.com:password456"}
							rows={6}
							value={inputText}
							onChange={(e) => setInputText(e.target.value)}
							className="font-mono text-sm"
							autoFocus
						/>
						<p className="text-muted-foreground text-xs">
							{lineCount} account{lineCount !== 1 ? "s" : ""}
						</p>
					</div>
				)}

				{/* Step 2b: manual tokens */}
				{step === "manual" && (
					<div className="flex flex-col gap-3 py-2">
						<label className="text-sm font-medium">
							API Keys / Tokens{" "}
							<span className="text-muted-foreground font-normal">(one per line)</span>
						</label>
						<Textarea
							placeholder={"eyJhbGci...(access_token)\nv1.MTI...(refresh_token)"}
							rows={6}
							value={inputText}
							onChange={(e) => setInputText(e.target.value)}
							className="font-mono text-xs"
							autoFocus
						/>
						<p className="text-muted-foreground text-xs">
							{lineCount} key{lineCount !== 1 ? "s" : ""}
						</p>
					</div>
				)}

				<DialogFooter className="gap-2 sm:gap-0">
					{step === "choose" ? (
						<Button variant="outline" onClick={handleClose}>
							Cancel
						</Button>
					) : (
						<>
							<Button variant="outline" onClick={() => setStep("choose")} disabled={isSubmitting}>
								Back
							</Button>
							<Button
								onClick={step === "auto" ? handleAutoLogin : handleManual}
								disabled={lineCount === 0 || isSubmitting}
								isLoading={isSubmitting}
							>
								Add {lineCount > 0 ? `(${lineCount} account${lineCount !== 1 ? "s" : ""})` : "Accounts"}
							</Button>
						</>
					)}
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}
