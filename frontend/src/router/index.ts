import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import VerificationView from '@/views/VerificationView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/transform_verification', name: 'transform_verification', component: VerificationView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
