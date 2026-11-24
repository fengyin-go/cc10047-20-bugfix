import { createRouter, createWebHistory } from 'vue-router'

import IncrementalReceiptView from '@/views/IncrementalReceiptView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/incremental_receipt', name: 'incremental-receipt', component: IncrementalReceiptView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
