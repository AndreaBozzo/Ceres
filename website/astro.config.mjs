// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Ceres',
			customCss: [
				// Add your custom styles here
				'./src/styles/custom.css',
			],
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/withastro/starlight' }],
			sidebar: [
				{
					label: 'Documentation',
					items: [
						{ label: 'Harvesting architecture', slug: 'harvesting' },
						{ label: 'Cost Analysis', slug: 'cost' },
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
