// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://learnceres.pages.dev',
	integrations: [
		starlight({
			title: 'Ceres',
			description:
				'Harvest-first Rust toolkit for open data portals: one synchronized catalog from 120+ portals, with optional embeddings, semantic search, and a published open data index.',
			customCss: [
				// Add your custom styles here
				'./src/styles/custom.css',
			],
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/AndreaBozzo/Ceres' },
				{ icon: 'discord', label: 'Discord', href: 'https://discord.gg/fztdKSPXSz' },
				{
					icon: 'seti:db',
					label: 'Open Data Index',
					href: 'https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index',
				},
			],
			editLink: {
				baseUrl: 'https://github.com/AndreaBozzo/Ceres/edit/master/website/',
			},
			sidebar: [
				{
					label: 'Documentation',
					items: [
						{ label: 'Supported portals', slug: 'portals' },
						{ label: 'Harvesting architecture', slug: 'harvesting' },
						{ label: 'Embeddings and costs', slug: 'cost' },
					],
				},
				{
					label: 'Community',
					items: [
						{ label: 'Contributing to Ceres', slug: 'contributing' },
						{ label: 'Security Policy', slug: 'security' },
						{ label: 'Code of Conduct', slug: 'code_of_conduct' },
					],
				},
			],
		}),
	],
});
